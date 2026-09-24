from evil.tools.nas_index import find_nas_files, register_nas_file


def test_register_and_find_all_files_for_a_run(conn):
    register_nas_file(conn, "run-1", "/nas/run1.bag", "rosbag", 0.0, 100.0)
    register_nas_file(conn, "run-1", "/nas/run1_cam.mp4", "video", 0.0, 100.0)
    register_nas_file(conn, "run-2", "/nas/run2.bag", "rosbag", 0.0, 50.0)

    files = find_nas_files(conn, "run-1")

    assert len(files) == 2
    assert {f["path"] for f in files} == {"/nas/run1.bag", "/nas/run1_cam.mp4"}


def test_find_nas_files_filters_by_overlapping_range(conn):
    register_nas_file(conn, "run-1", "/nas/part1.bag", "rosbag", 0.0, 50.0)
    register_nas_file(conn, "run-1", "/nas/part2.bag", "rosbag", 50.0, 100.0)

    # a turn at [40, 45] overlaps only part1
    files = find_nas_files(conn, "run-1", start_ts=40.0, end_ts=45.0)
    assert [f["path"] for f in files] == ["/nas/part1.bag"]

    # a turn at [60, 65] overlaps only part2
    files = find_nas_files(conn, "run-1", start_ts=60.0, end_ts=65.0)
    assert [f["path"] for f in files] == ["/nas/part2.bag"]

    # a turn spanning the boundary overlaps both
    files = find_nas_files(conn, "run-1", start_ts=45.0, end_ts=55.0)
    assert {f["path"] for f in files} == {"/nas/part1.bag", "/nas/part2.bag"}


def test_find_nas_files_unknown_run_returns_empty(conn):
    assert find_nas_files(conn, "no-such-run") == []
