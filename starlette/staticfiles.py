from __future__ import annotations

import errno
import importlib.util
import os
import stat
import sys
from email.utils import parsedate
from typing import IO, Union

import anyio
import anyio.to_thread

from starlette._utils import get_route_path
from starlette.datastructures import URL, Headers
from starlette.exceptions import HTTPException
from starlette.responses import FileResponse, RedirectResponse, Response
from starlette.types import Receive, Scope, Send
from starlette.websockets import WebSocketClose

PathLike = Union[str, "os.PathLike[str]"]


class NotModifiedResponse(Response):
    NOT_MODIFIED_HEADERS = (
        "cache-control",
        "content-location",
        "date",
        "etag",
        "expires",
        "vary",
    )

    def __init__(self, headers: Headers):
        super().__init__(
            status_code=304,
            headers={name: value for name, value in headers.items() if name in self.NOT_MODIFIED_HEADERS},
        )


class StaticFiles:
    def __init__(
        self,
        *,
        directory: PathLike | None = None,
        packages: list[str | tuple[str, str]] | None = None,
        html: bool = False,
        check_dir: bool = True,
        follow_symlink: bool = False,
    ) -> None:
        self.directory = directory
        self.packages = packages
        self.all_directories = self.get_directories(directory, packages)
        self.html = html
        self.config_checked = False
        self.follow_symlink = follow_symlink
        if check_dir and directory is not None and not os.path.isdir(directory):
            raise RuntimeError(f"Directory '{directory}' does not exist")

    def get_directories(
        self,
        directory: PathLike | None = None,
        packages: list[str | tuple[str, str]] | None = None,
    ) -> list[PathLike]:
        """
        Given `directory` and `packages` arguments, return a list of all the
        directories that should be used for serving static files from.
        """
        directories = []
        if directory is not None:
            directories.append(directory)

        for package in packages or []:
            if isinstance(package, tuple):
                package, statics_dir = package
            else:
                statics_dir = "statics"
            spec = importlib.util.find_spec(package)
            assert spec is not None, f"Package {package!r} could not be found."
            assert spec.origin is not None, f"Package {package!r} could not be found."
            package_directory = os.path.normpath(os.path.join(spec.origin, "..", statics_dir))
            assert os.path.isdir(package_directory), (
                f"Directory '{statics_dir!r}' in package {package!r} could not be found."
            )
            directories.append(package_directory)

        return directories

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """
        The ASGI entry point.
        """
        if scope["type"] == "websocket":
            websocket_close = WebSocketClose()
            await websocket_close(scope, receive, send)
            return

        assert scope["type"] == "http"

        if not self.config_checked:
            await self.check_config()
            self.config_checked = True

        path = self.get_path(scope)
        response = await self.get_response(path, scope)
        await response(scope, receive, send)

    def get_path(self, scope: Scope) -> str:
        """
        Given the ASGI scope, return the `path` string to serve up,
        with OS specific path separators, and any '..', '.' components removed.
        """
        route_path = get_route_path(scope)
        return os.path.normpath(os.path.join(*route_path.split("/")))

    async def get_response(self, path: str, scope: Scope) -> Response:
        """
        Returns an HTTP response, given the incoming path, method and request headers.
        """
        if scope["method"] not in ("GET", "HEAD"):
            raise HTTPException(status_code=405)

        try:
            file, full_path, stat_result = await anyio.to_thread.run_sync(self.lookup_file, path)
        except PermissionError:
            raise HTTPException(status_code=401)
        except OSError as exc:
            # Filename is too long, so it can't be a valid static file.
            if exc.errno == errno.ENAMETOOLONG:
                raise HTTPException(status_code=404)

            raise exc
        except ValueError:
            # Null bytes or other invalid characters in the path.
            raise HTTPException(status_code=404)

        if file is not None:
            # We have a static file to serve.  The file is already open, so
            # its stat result and the bytes sent to the client describe the
            # very same file, even if the directory tree changes while the
            # response is being built or transmitted.
            assert stat_result is not None
            return self.file_response(file, full_path, stat_result, scope)

        if stat_result is not None and stat.S_ISDIR(stat_result.st_mode) and self.html:
            # We're in HTML mode, and have got a directory URL.
            # Check if we have 'index.html' file to serve.
            index_path = os.path.join(path, "index.html")
            file, full_path, stat_result = await anyio.to_thread.run_sync(self.lookup_file, index_path)
            if file is not None:
                assert stat_result is not None
                if not scope["path"].endswith("/"):
                    # Directory URLs should redirect to always end in "/".
                    url = URL(scope=scope)
                    url = url.replace(path=url.path + "/")
                    file.close()
                    return RedirectResponse(url=url)
                return self.file_response(file, full_path, stat_result, scope)

        if self.html:
            # Check for '404.html' if we're in HTML mode.
            file, full_path, stat_result = await anyio.to_thread.run_sync(self.lookup_file, "404.html")
            if file is not None:
                assert stat_result is not None
                return FileResponse(full_path, file=file, stat_result=stat_result, status_code=404)
        raise HTTPException(status_code=404)

    def lookup_path(self, path: str) -> tuple[str, os.stat_result | None]:
        # Reject absolute paths so they cannot escape the served directory.
        if path.startswith(("/", "\\")):
            return "", None
        for directory in self.all_directories:
            joined_path = os.path.join(directory, path)
            if self.follow_symlink:
                full_path = os.path.abspath(joined_path)
                directory = os.path.abspath(directory)
            else:
                full_path = os.path.realpath(joined_path)
                directory = os.path.realpath(directory)
            if os.path.commonpath([full_path, directory]) != str(directory):
                # Don't allow misbehaving clients to break out of the static files directory.
                continue
            try:
                return full_path, os.stat(full_path)
            except (FileNotFoundError, NotADirectoryError):
                continue
        return "", None

    def lookup_file(
        self, path: str
    ) -> tuple[IO[bytes] | None, str, os.stat_result | None]:
        """
        Resolve ``path`` within the configured directories and return an
        already-open binary file object together with its stat result when
        it names a regular file.

        Path resolution is the same as in :meth:`lookup_path`, but in
        addition the file is opened and the opened descriptor is verified
        to still reside inside the served directory.  Binding the access
        check to an open file descriptor (rather than to a path string)
        ensures the response metadata and the bytes that are eventually sent
        describe the same file.  Replacing a directory or swapping in a
        symlink after this lookup cannot redirect the open descriptor to
        content outside the served tree.

        Directories are returned as ``(None, full_path, stat_result)`` so
        html mode can still discover and redirect to them.
        """
        resolved_path, _ = self.lookup_path(path)
        if not resolved_path:
            return None, "", None

        root = self._root_for(resolved_path)
        if root is None:
            return None, "", None

        try:
            fd = self._open_fd(resolved_path)
        except (FileNotFoundError, NotADirectoryError):
            # The tree changed between resolution and opening; the request
            # can simply fail as if the file had never existed.
            return None, "", None

        file = None
        try:
            if not self.follow_symlink and not self._fd_is_within(fd, root):
                # The opened descriptor resolves outside of the served
                # directory: a directory component was renamed or a symlink
                # was swapped in between path resolution and opening.
                return None, "", None

            stat_result = os.fstat(fd)
            if stat.S_ISDIR(stat_result.st_mode):
                # Directories are reported without a file so html mode can
                # discover and redirect to them.
                return None, resolved_path, stat_result
            if not stat.S_ISREG(stat_result.st_mode):
                return None, "", None

            # Wrap the descriptor; ownership is transferred to the caller,
            # which closes it once the response has been sent.
            file = os.fdopen(fd, "rb")
            return file, resolved_path, stat_result
        finally:
            if file is None:
                # fdopen() did not take ownership of the descriptor.
                os.close(fd)

    def _root_for(self, resolved_path: str) -> str | None:
        """
        Return the configured directory containing ``resolved_path``, after
        both sides have been canonicalized in the same way.
        """
        for directory in self.all_directories:
            root = os.path.abspath(directory) if self.follow_symlink else os.path.realpath(directory)
            try:
                if os.path.commonpath([resolved_path, root]) == root:
                    return root
            except ValueError:
                continue
        return None

    def _open_fd(self, full_path: str) -> int:
        """
        Open ``full_path`` for reading, returning its file descriptor.  When
        symlinks are disabled the final path component is not followed.
        """
        flags = os.O_RDONLY
        if not self.follow_symlink and sys.platform != "win32" and hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            return os.open(full_path, flags)
        except OSError as exc:
            if (
                not self.follow_symlink
                and sys.platform != "win32"
                and hasattr(errno, "ELOOP")
                and exc.errno == errno.ELOOP
            ):
                # The final component is a symlink.  Resolve it explicitly;
                # the descriptor is then validated against the served
                # directory.  Symlinks to files *inside* the tree therefore
                # keep working while follow_symlink is disabled, while a
                # link pointing (or switched) outside of it is rejected.
                return os.open(full_path, os.O_RDONLY)
            raise

    def _fd_is_within(self, fd: int, root: str) -> bool:
        """
        Return whether the file backing an open descriptor resides within
        ``root``.

        On Linux the descriptor's kernel-managed path (``/proc/self/fd``)
        is inspected: unlike re-statting a path string, it cannot be
        fooled by renaming directories or replacing symlinks after the
        file was opened.  Elsewhere the check falls back to ``O_NOFOLLOW``
        semantics established while opening.
        """
        resolved = self._fd_realpath(fd)
        if resolved is None:
            # No descriptor-based path is available; the O_NOFOLLOW open
            # already prevented the final component from being a link.
            return True
        try:
            return os.path.commonpath([resolved, root]) == root
        except ValueError:
            return False

    @staticmethod
    def _fd_realpath(fd: int) -> str | None:
        if sys.platform.startswith("linux"):
            candidates = (f"/proc/self/fd/{fd}",)
        elif sys.platform == "darwin" or sys.platform.startswith(("freebsd", "netbsd", "openbsd")):
            candidates = (f"/dev/fd/{fd}",)
        else:
            return None
        for candidate in candidates:
            try:
                return os.path.realpath(candidate)
            except OSError:
                continue
        return None

    def file_response(
        self,
        file: IO[bytes],
        full_path: PathLike,
        stat_result: os.stat_result,
        scope: Scope,
        status_code: int = 200,
    ) -> Response:
        request_headers = Headers(scope=scope)

        response = FileResponse(full_path, file=file, status_code=status_code, stat_result=stat_result)
        if self.is_not_modified(response.headers, request_headers):
            # No body will be sent, so the checked file is no longer needed.
            file.close()
            return NotModifiedResponse(response.headers)
        return response

    async def check_config(self) -> None:
        """
        Perform a one-off configuration check that StaticFiles is actually
        pointed at a directory, so that we can raise loud errors rather than
        just returning 404 responses.
        """
        if self.directory is None:
            return

        try:
            stat_result = await anyio.to_thread.run_sync(os.stat, self.directory)
        except FileNotFoundError:
            raise RuntimeError(f"StaticFiles directory '{self.directory}' does not exist.")
        if not (stat.S_ISDIR(stat_result.st_mode) or stat.S_ISLNK(stat_result.st_mode)):
            raise RuntimeError(f"StaticFiles path '{self.directory}' is not a directory.")

    def is_not_modified(self, response_headers: Headers, request_headers: Headers) -> bool:
        """
        Given the request and response headers, return `True` if an HTTP
        "Not Modified" response could be returned instead.
        """
        if if_none_match := request_headers.get("if-none-match"):
            if if_none_match.strip() == "*":
                return True
            # The "etag" header is added by FileResponse, so it's always present.
            etag = response_headers["etag"]
            return etag in [tag.strip().removeprefix("W/") for tag in if_none_match.split(",")]

        try:
            if_modified_since = parsedate(request_headers["if-modified-since"])
            last_modified = parsedate(response_headers["last-modified"])
            if if_modified_since is not None and last_modified is not None and if_modified_since >= last_modified:
                return True
        except KeyError:
            pass

        return False
