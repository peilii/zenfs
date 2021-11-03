#include "utils.h"

namespace ROCKSDB_NAMESPACE {

int test_mount_zenfs() {
    return 0;
}

int test_writable_file() {
    // Create a writable file and append
    std::shared_ptr<Logger> logger;
    Status s;
    char f[100] = {0};
    FileOptions fopts;
    std::unique_ptr<FSWritableFile> f_file;
    IOOptions iopts;
    IODebugContext dbg;

    char buffer[1048576] = {0};
    Slice slice(buffer, 1048576);

    s = Env::Default()->NewLogger(GetLogFilename(FLAGS_zbd), &logger);
    if (! s.ok()) {
        fprintf(stderr, "ZenFS - ByteDisk: Could not create logger.");
        return -1;
    }

    logger->SetInfoLogLevel(DEBUG_LEVEL);

    ZonedBlockDevice * zbd = zbd_open(false, logger);
    if (zbd == nullptr) {
        fprintf(stderr, "ZenFS - ByteDisk: Could not open device.");
        return -1;
    }

    ZenFS *zenFS;
    s = zenfs_mount(zbd, &zenFS, false, logger);
    if (! s.ok()) {
        fprintf(stderr, "ZenFS - ByteDisk: Could not mount ZenFS: %s\n", s.ToString());
        return -1;
    }
    
    s = zenFS->NewWritableFile(f, fopts, &f_file, &dbg);
    if (!s.ok()) {
        fprintf(stderr, "ZenFS - ByteDisk: Could not create writeable file: %s\n", s.ToString());
        return -1;
    }

    s = f_file->Append(slice, iopts, &dbg);
    if (!s.ok()) {
        fprintf(stderr, "ZenFS - ByteDisk: Could not append writeable file: %s\n", s.ToString());
        return -1;
    }

    s = f_file->Sync(iopts, &dbg);
    if (!s.ok()) {
        fprintf(stderr, "ZenFS - ByteDisk: Could not sync writeable file: %s\n", s.ToString());
        return -1;
    }
    // rename file

    f = {1};
    s = f_file->Rename(f);
    if (!s.ok()) {
        fprintf(stderr, "ZenFS - ByteDisk: Could not rename writeable file: %s\n", s.ToString());
        return -1;
    }

    s = zenFS->FileExists(f, iopts, &dbg);
    if (!s.ok()) {
        fprintf(stderr, "ZenFS - ByteDisk: Could not check exists writeable file: %s\n", s.ToString());
        return -1;
    }

    // delete file
    s = zenFS->DeleteFile(f);
    if (!s.ok()) {
        fprintf(stderr, "ZenFS - ByteDisk: Could not delete writeable file: %s\n", s.ToString());
        return -1;
    }
    
    s = zenFS->FileExists(f, iopts, &dbg);
    if (!s.ok()) {
        fprintf(stderr, "ZenFS - ByteDisk: Could not check exists writeable file: %s\n", s.ToString());
        return -1;
    }

    return 0;
}

}  // namespace ROCKSDB_NAMESPACE

int main() {
    gflags::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                          +" <command> [OPTIONS]...\nCommands: mkfs, list, "
                           "ls-uuid, df, backup, restore");

    gflags::ParseCommandLineFlags(&argc, &argv, true);

#ifdef ZENFS_BYTEDISK
    return ROCKSDB_NAMESPACE::test_writable_file();
#endif
    return 0;
}
