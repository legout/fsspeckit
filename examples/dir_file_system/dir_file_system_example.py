from fsspeckit import filesystem


def demo_local():
    """Demonstrate local Directory FileSystem."""
    # Local Directory FileSystem.
    fs_dir_local = filesystem("./my_local_dir/", dirfs=True)
    print(f"Local DirFileSystem: {fs_dir_local}")


def demo_s3():
    """Demonstrate S3 Directory FileSystem (requires s3fs)."""
    # S3 Directory FileSystem for my-bucket. dirfs=True is optional, as it is
    # the default behavior for paths ending with a slash.
    # Replace "my-bucket" with your actual S3 bucket name.
    fs_dir_s3 = filesystem("s3://my-bucket", dirfs=True)

    # S3 Directory FileSystem with storage_options
    fs_dir_s3_so = filesystem(
        "s3://my-bucket", storage_options={"key": "your_key", "secret": "your_secret"}
    )

    print(f"S3 DirFileSystem (default): {fs_dir_s3}")
    print(f"S3 DirFileSystem (with storage_options): {fs_dir_s3_so}")


def main():
    """Run local and conditional S3 DirFileSystem demonstrations."""
    print("=== DirFileSystem Examples ===\n")

    print("--- Local DirFileSystem ---")
    demo_local()

    try:
        import s3fs  # noqa: F401
    except ImportError:
        print("\ns3fs is not installed. Skipping S3 DirFileSystem demo.")
        print("Install with: pip install 'fsspeckit[aws]'")
        return

    print("\n--- S3 DirFileSystem ---")
    demo_s3()


if __name__ == "__main__":
    main()
