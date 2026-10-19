import Foundation
import Darwin

/// A per-user process lock shared by installed and development copies. Retaining
/// the descriptor holds ownership; the kernel releases it even after a crash.
final class MenuBarInstanceLock {
    private var descriptor: Int32 = -1

    func acquire(at url: URL) throws -> Bool {
        precondition(descriptor == -1)
        try FileManager.default.createDirectory(at: url.deletingLastPathComponent(),
            withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        let handle = open(url.path, O_CREAT | O_RDWR | O_NOFOLLOW, 0o600)
        guard handle >= 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
        if flock(handle, LOCK_EX | LOCK_NB) != 0 {
            let code = errno
            close(handle)
            if code == EWOULDBLOCK { return false }
            throw POSIXError(POSIXErrorCode(rawValue: code) ?? .EIO)
        }
        descriptor = handle
        return true
    }

    func release() {
        if descriptor >= 0 { close(descriptor); descriptor = -1 }
    }

    deinit { release() }
}
