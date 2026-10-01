from __future__ import annotations

import errno
import importlib.util
import os
import stat
from email.utils import parsedate
from typing import IO, Literal, Union

import anyio
import anyio.to_thread

from starlette._utils import get_route_path
from starlette.datastructures import URL, Headers
from starlette.exceptions import HTTPException
from starlette.responses import FileResponse, RedirectResponse, Response
from starlette.types import Receive, Scope, Send
from starlette.websockets import WebSocketClose

PathLike = Union[str, "os.PathLike[str]"]

# Outcome of the race-free fd-relative lookup: a pinned regular-file
# descriptor, a directory stat result, or no served path at all.
LookupResult = tuple[Literal["file"], int] | tuple[Literal["dir"], os.stat_result] | tuple[Literal["missing"], None]

# Number of symlink expansions allowed while resolving a single path. This is
# the same budget Linux itself uses for path lookup (MAX_NESTED_LINKS = 40).
_MAX_SYMLINKS = 40

# Race-free, fd-relative lookup is only available on POSIX systems where the
# underlying open(2) supports the O_NOFOLLOW flag. On other platforms (notably
# Windows) we fall back to the path-string based checks.
_FD_LOOKUP = hasattr(os, "O_NOFOLLOW") and hasattr(os, "O_DIRECTORY") and hasattr(os, "open")


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
            full_path, stat_result, file = await anyio.to_thread.run_sync(self._lookup_path_pinned, path)
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

        if stat_result and stat.S_ISREG(stat_result.st_mode):
            # We have a static file to serve. On platforms that support
            # fd-relative open(2), `file` is the handle of the very inode that
            # was checked, so the response can never resolve a different file
            # at transfer time; elsewhere it is None and the path is opened
            # directly as before.
            return self.file_response(full_path, stat_result, scope, file=file)

        # A regular file cannot resolve to a directory, so an open handle here
        # can only belong to a file that is no longer the right response.
        if file is not None:
            file.close()

        if stat_result and stat.S_ISDIR(stat_result.st_mode) and self.html:
            # We're in HTML mode, and have got a directory URL.
            # Check if we have 'index.html' file to serve.
            index_path = os.path.join(path, "index.html")
            index_path, stat_result, file = await anyio.to_thread.run_sync(self._lookup_path_pinned, index_path)
            if stat_result is not None and stat.S_ISREG(stat_result.st_mode):
                if not scope["path"].endswith("/"):
                    # Directory URLs should redirect to always end in "/".
                    if file is not None:
                        file.close()
                    url = URL(scope=scope)
                    url = url.replace(path=url.path + "/")
                    return RedirectResponse(url=url)
                return self.file_response(index_path, stat_result, scope, file=file)
            if file is not None:
                file.close()

        if self.html:
            # Check for '404.html' if we're in HTML mode.
            full_path, stat_result, file = await anyio.to_thread.run_sync(self._lookup_path_pinned, "404.html")
            if stat_result and stat.S_ISREG(stat_result.st_mode):
                return FileResponse(full_path, stat_result=stat_result, status_code=404, file=file)
            if file is not None:
                file.close()
        raise HTTPException(status_code=404)

    def lookup_path(self, path: str) -> tuple[str, os.stat_result | None]:
        full_path, stat_result, file = self._lookup_path_pinned(path)
        # The 2-tuple API does not hand the pinned handle to the caller.
        if file is not None:
            file.close()
        return full_path, stat_result

    def _lookup_path_pinned(self, path: str) -> tuple[str, os.stat_result | None, IO[bytes] | None]:
        # Reject absolute paths so they cannot escape the served directory.
        if path.startswith(("/", "\\")):
            return "", None, None
        for directory in self.all_directories:
            joined_path = os.path.join(directory, path)
            if self.follow_symlink:
                root = os.path.abspath(directory)
                full_path = os.path.abspath(joined_path)
                directory = root
            else:
                root = os.path.realpath(directory)
                full_path = os.path.realpath(joined_path)
                directory = root
            if os.path.commonpath([full_path, directory]) != str(directory):
                # Don't allow misbehaving clients to break out of the static files directory.
                continue

            file: IO[bytes] | None = None
            try:
                if _FD_LOOKUP:
                    # Resolve and open the file in a single fd-relative walk.
                    # Holding the resulting descriptor pins the exact inode
                    # that passed the boundary checks above; renames or
                    # symlink swaps happening afterwards cannot affect this
                    # request. The walk itself decides between "regular file",
                    # "directory" and "does not exist", so no path string is
                    # re-stat'ed or re-opened afterwards.
                    kind, result = self._race_free_lookup(root, path)
                    if kind == "file":
                        assert isinstance(result, int)
                        try:
                            file = os.fdopen(result, "rb")
                        except BaseException:
                            os.close(result)
                            raise
                        stat_result: os.stat_result | None = os.fstat(result)
                    elif kind == "dir":
                        assert isinstance(result, os.stat_result)
                        stat_result = result
                    else:
                        continue
                else:
                    stat_result = os.stat(full_path)
            except (FileNotFoundError, NotADirectoryError):
                if file is not None:
                    file.close()
                continue
            except OSError:
                if file is not None:
                    file.close()
                raise

            return full_path, stat_result, file
        return "", None, None

    def _race_free_lookup(self, root: str, path: str) -> LookupResult:
        """
        Resolve ``os.path.join(root, path)`` and return the target pinned to an
        open descriptor, so later renames or symlink swaps cannot change what
        the request serves.

        Returns one of:
        * ``("file", fd)``      -- a regular file, with an open descriptor that
                                   pins the exact inode that was validated;
        * ``("dir", stat)``     -- a directory, with its fstat result;
        * ``("missing", None)`` -- the path does not resolve to a served file.

        With `follow_symlink` enabled the kernel performs the whole resolution
        atomically in one open(2) (links may point outside the root; their
        lexical path already passed containment in `lookup_path`). Without it,
        the path is walked with O_NOFOLLOW + openat and every link is expanded
        manually and re-checked against the root, reproducing the boundary
        semantics of ``realpath`` + ``commonpath`` with no check/use gap.
        """
        if self.follow_symlink:
            return self._lookup_following(root, path)
        return self._lookup_restricted(root, path)

    @staticmethod
    def _openat(dir_fd: int, name: str, no_follow: bool) -> int:
        flags = os.O_RDONLY
        if no_follow:
            flags |= os.O_NOFOLLOW
        return os.open(name, flags, dir_fd=dir_fd)

    @staticmethod
    def _split_components(path: str) -> list[str]:
        # get_path already normalized the request; split on the OS path
        # separator. On POSIX a backslash is a valid filename character and
        # must not be treated as a separator.
        return [component for component in path.replace("/", os.sep).split(os.sep) if component and component != "."]

    def _lookup_following(self, root: str, path: str) -> LookupResult:
        full_path = os.path.abspath(os.path.join(root, path))
        try:
            fd = os.open(full_path, os.O_RDONLY)
        except (FileNotFoundError, NotADirectoryError):
            return "missing", None
        st = os.fstat(fd)
        if stat.S_ISDIR(st.st_mode):
            os.close(fd)
            return "dir", st
        if stat.S_ISREG(st.st_mode):
            # Ownership of the descriptor transfers to the caller.
            return "file", fd
        os.close(fd)
        return "missing", None

    def _lookup_restricted(self, root: str, path: str) -> LookupResult:
        root_realpath = os.path.realpath(root)
        root_fd = os.open(root_realpath, os.O_RDONLY | os.O_DIRECTORY)
        # One entry per physical directory component of the resolved path:
        # (base_fd, own_fd). `base_fd` is the descriptor of the directory this
        # component was reached from (never closed by this entry -- owned by
        # the entry below or by root_fd); `own_fd` is the descriptor opened by
        # this entry, closed on rewind/cleanup.
        #
        # A symlink component occupies no entry: per the kernel's path-lookup
        # rules its target text is spliced in at the link's position and
        # resolved relative to the directory containing the link (the current
        # `fd`), so a leading ".." rewinds that containing directory exactly
        # as realpath() would. Absolute symlinks must remain inside the root
        # and restart the walk from the root descriptor.
        stack: list[tuple[int, int]] = []
        try:
            pending: list[str] = self._split_components(path)
            if not pending:
                return "dir", os.fstat(root_fd)
            resolutions = 0
            while pending:
                component = pending.pop(0)
                if component == "..":
                    if stack:
                        _, own_fd = stack.pop()
                        os.close(own_fd)
                    continue

                fd = stack[-1][1] if stack else root_fd
                next_fd: int | None = None
                st: os.stat_result | None = None
                target: str | None = None
                try:
                    next_fd = self._openat(fd, component, True)
                except FileNotFoundError:
                    return "missing", None
                except NotADirectoryError:
                    return "missing", None
                except OSError as exc:
                    if exc.errno != getattr(errno, "ELOOP", -1):
                        raise
                    # O_NOFOLLOW: the component is a symlink.
                    target = os.readlink(component, dir_fd=fd)
                else:
                    st = os.fstat(next_fd)
                    if stat.S_ISLNK(st.st_mode):
                        # O_NOFOLLOW normally raises ELOOP here; handle the
                        # platform that reports the link itself explicitly.
                        target = os.readlink(component, dir_fd=fd)

                if target is not None:
                    if next_fd is not None:
                        os.close(next_fd)
                    resolutions += 1
                    if resolutions > _MAX_SYMLINKS:
                        return "missing", None
                    if target.startswith(("/", "\\")):
                        # Only allow the absolute target if the lexically
                        # normalized destination (including remaining
                        # components) stays under the root, then restart the
                        # walk from the root descriptor.
                        normalized = os.path.normpath(os.path.join(target, *pending))
                        if os.path.commonpath([normalized, root_realpath]) != root_realpath:
                            return "missing", None
                        while stack:
                            os.close(stack.pop()[1])
                        pending = self._split_components(normalized[len(root_realpath) :])
                    else:
                        pending = self._split_components(target) + pending
                    continue

                assert st is not None
                assert next_fd is not None
                if stat.S_ISDIR(st.st_mode):
                    stack.append((fd, next_fd))
                    continue

                if pending:
                    # A non-directory component before the end of the path
                    # (e.g. file/child), matching ENOTDIR semantics.
                    os.close(next_fd)
                    return "missing", None
                if not stat.S_ISREG(st.st_mode):
                    os.close(next_fd)
                    return "missing", None
                # Ownership of the file descriptor transfers to the caller.
                return "file", next_fd
            return "dir", os.fstat(stack[-1][1] if stack else root_fd)
        finally:
            while stack:
                os.close(stack.pop()[1])
            os.close(root_fd)

    def file_response(
        self,
        full_path: PathLike,
        stat_result: os.stat_result,
        scope: Scope,
        status_code: int = 200,
        file: IO[bytes] | None = None,
    ) -> Response:
        request_headers = Headers(scope=scope)

        response = FileResponse(full_path, status_code=status_code, stat_result=stat_result, file=file)
        if self.is_not_modified(response.headers, request_headers):
            # The pinned handle is not needed for a header-only response;
            # FileResponse will not be called, so close it here.
            if file is not None:
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
