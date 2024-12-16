import json
import os
import re
import shutil
import tempfile
from unittest.mock import Mock, call, mock_open, patch

import pytest

from taca.nanopore import instrument_transfer

DUMMY_RUN_NAME = "20240112_2342_MN19414_TEST12345_randomhash"


@pytest.fixture
def setup_test_fixture():
    """Set up tempdir to mimic an ONT instrument file system"""

    tmp = tempfile.TemporaryDirectory()

    # Set up args
    args = Mock()
    args.source_dir = tmp.name + "/data"
    args.dest_dir = tmp.name + "/preproc"
    args.dest_dir_qc = tmp.name + "/preproc/qc"
    args.archive_dir = tmp.name + "/data/nosync"
    args.minknow_logs_dir = tmp.name + "/minknow_logs"
    args.log_path = args.source_dir + "/instrument_transfer_log.txt"

    # Create dirs
    for dir in [
        args.source_dir,
        args.dest_dir,
        args.dest_dir + "/nosync",
        args.dest_dir + "/nosync/archived",
        args.dest_dir_qc,
        args.archive_dir,
        args.minknow_logs_dir,
    ]:
        os.makedirs(dir)

    # Create files
    file_paths = {
        "rsync_log_path": args.source_dir + "/rsync_log.txt",
        "script_log_path": args.source_dir + "/instrument_transfer_log.txt",
    }
    for file_path in file_paths:
        open(file_paths[file_path], "w").close()

    # Build log dirs
    for position_dir_n, position_dir in enumerate(["1A", "MN19414"]):
        os.makedirs(tmp.name + f"/minknow_logs/{position_dir}")

        # Build log files
        for log_file_n, log_file in enumerate(
            ["control_server_log-1.txt", "control_server_log-2.txt"]
        ):
            # For each flowcell
            for flowcell_n, flowcell in enumerate(["PAM12345", "TEST12345"]):
                # Build log entries
                for log_entry_n, log_entry in enumerate(
                    ["mux_scan_result", "platform_qc.report", "something.else"]
                ):
                    # Sneak build metadata into log entries to retain traceability
                    lines = [
                        f"2024-01-01 0{position_dir_n}:0{log_file_n}:0{flowcell_n}.0{log_entry_n}    INFO: {log_entry} (user_messages)",
                        f"    flow_cell_id: {flowcell}",
                        f"    num_pores: {position_dir_n}{log_file_n}{flowcell_n}{log_entry_n}",
                        f"    total_pores: {position_dir_n}{log_file_n}{flowcell_n}{log_entry_n}",
                    ]

                    with open(
                        args.minknow_logs_dir + f"/{position_dir}/{log_file}", "a"
                    ) as file:
                        file.write("\n".join(lines) + "\n")

    yield args, tmp, file_paths

    tmp.cleanup()


def test_main_delete(setup_test_fixture):
    """Check so that remotely archived runs runs and empty dirs
    are deleted from the local archive.
    """

    # Run fixture
    args, tmp, file_paths = setup_test_fixture

    # Locally and remotely archived run
    run_path = f"{args.archive_dir}/experiment/sample/{DUMMY_RUN_NAME}"
    os.makedirs(run_path)
    remote_run_path = f"{args.dest_dir}/nosync/{DUMMY_RUN_NAME}"
    os.makedirs(remote_run_path)

    # Locally but not remotely archived run
    innocent_run_path = f"{args.archive_dir}/innocent_experiment/innocent_sample/{DUMMY_RUN_NAME.replace('randomhash', 'dontDeleteMe')}"
    os.makedirs(innocent_run_path)

    with (
        patch("shutil.rmtree", side_effect=shutil.rmtree) as mock_rmtree,
        patch("os.rmdir", side_effect=os.rmdir) as mock_rmdir,
    ):
        # Run code
        instrument_transfer.main(args)

        # Assert deletions
        mock_rmtree.assert_has_calls([call(f"{run_path}")])
        mock_rmdir.assert_has_calls(
            [
                call(f"{args.archive_dir}/experiment/sample"),
                call(f"{args.archive_dir}/experiment"),
            ]
        )
        assert os.path.exists(innocent_run_path)


def test_main_ignore_CTC(setup_test_fixture):
    """Check so that runs on configuration test cells are not picked up."""

    # Run fixture
    args, tmp, file_paths = setup_test_fixture

    # Setup run
    run_path = (
        f"{args.source_dir}/experiment/sample/{DUMMY_RUN_NAME.replace('TEST', 'CTC')}"
    )
    os.makedirs(run_path)

    with patch("taca.nanopore.instrument_transfer.dump_path") as mock_dump_path:
        # Start testing
        instrument_transfer.main(args)

        # Check dump_path was not called
        mock_dump_path.assert_not_called()


@pytest.mark.parametrize(
    "finished, qc", [(True, True), (True, False), (False, True), (False, False)]
)
@patch("taca.nanopore.instrument_transfer.final_sync_to_storage")
@patch("taca.nanopore.instrument_transfer.sync_to_storage")
def test_main(mock_sync, mock_final_sync, setup_test_fixture, finished, qc):
    # Run fixture
    args, tmp, file_paths = setup_test_fixture

    # Set up ONT run
    if not qc:
        run_path = f"{args.source_dir}/experiment/sample/{DUMMY_RUN_NAME}"
    else:
        run_path = f"{args.source_dir}/experiment/QC_sample/{DUMMY_RUN_NAME}"
    os.makedirs(run_path)

    # Add finished file indicator
    if finished:
        open(run_path + "/final_summary.txt", "w").close()

    # Start testing
    instrument_transfer.main(args)

    if not qc:
        dest_path = args.dest_dir
    else:
        dest_path = args.dest_dir_qc

    # Check sync was initiated
    if not finished:
        mock_sync.assert_called_once_with(
            run_path, dest_path, file_paths["rsync_log_path"]
        )
    else:
        mock_final_sync.assert_called_once_with(
            run_path, dest_path, args.archive_dir, file_paths["rsync_log_path"]
        )

    # Check path was dumped
    assert os.path.exists(run_path + "/run_path.txt")
    assert open(run_path + "/run_path.txt").read() == "/".join(run_path.split("/")[-3:])

    # Check pore count history was dumped
    assert os.path.exists(run_path + "/pore_count_history.csv")
    # Assert that all relevant entries from all files from all dirs were dumped
    template = (
        "\n".join(
            [
                "flow_cell_id,timestamp,position,type,num_pores,total_pores",
                "TEST12345,2024-01-01 01:01:01.01,MN19414,qc,1111,1111",
                "TEST12345,2024-01-01 01:01:01.00,MN19414,mux,1110,1110",
                "TEST12345,2024-01-01 01:00:01.01,MN19414,qc,1011,1011",
                "TEST12345,2024-01-01 01:00:01.00,MN19414,mux,1010,1010",
                "TEST12345,2024-01-01 00:01:01.01,1A,qc,0111,0111",
                "TEST12345,2024-01-01 00:01:01.00,1A,mux,0110,0110",
                "TEST12345,2024-01-01 00:00:01.01,1A,qc,0011,0011",
                "TEST12345,2024-01-01 00:00:01.00,1A,mux,0010,0010",
            ]
        )
        + "\n"
    )
    assert open(run_path + "/pore_count_history.csv").read() == template


def test_sequencing_finished():
    with patch("os.listdir") as mock_listdir:
        mock_listdir.return_value = ["file1", "file2", "final_summary"]
        assert instrument_transfer.sequencing_finished("path") is True

        mock_listdir.return_value = ["file1", "file2"]
        assert instrument_transfer.sequencing_finished("path") is False


def test_dump_path():
    with patch("builtins.open", new_callable=mock_open) as mock_file:
        instrument_transfer.dump_path("path/to/run")
        mock_file.assert_called_once_with("path/to/run/run_path.txt", "w")


def test_write_finished_indicator():
    with patch("builtins.open", new_callable=mock_open) as mock_file:
        run_path = "path/to/run"
        instrument_transfer.write_finished_indicator(run_path)
        mock_file.assert_called_once_with(run_path + "/.sync_finished", "w")


def test_sync_to_storage():
    with patch("subprocess.Popen") as mock_Popen:
        instrument_transfer.sync_to_storage("run_dir", "destination", "log")
        mock_Popen.assert_called_once_with(
            [
                "run-one",
                "rsync",
                "-rvu",
                "--log-file=" + "log",
                "run_dir",
                "destination",
            ]
        )


@patch("taca.nanopore.instrument_transfer.archive_finished_run")
@patch("taca.nanopore.instrument_transfer.write_finished_indicator")
@patch("subprocess.run")
def test_final_sync_to_storage(
    mock_run,
    mock_write_finished_indicator,
    mock_archive_finished_run,
):
    mock_write_finished_indicator.return_value = ".sync_finished"

    # For finished run
    mock_run.return_value.returncode = 0

    instrument_transfer.final_sync_to_storage(
        run_dir="run_dir",
        destination="destination",
        archive_dir="archive_dir",
        rsync_log="log_path",
    )

    assert mock_run.call_args_list[0] == call(
        [
            "run-one",
            "rsync",
            "-rvu",
            "--log-file=" + "log_path",
            "run_dir",
            "destination",
        ]
    )

    assert mock_run.call_args_list[1] == call(
        ["rsync", ".sync_finished", "destination/run_dir"]
    )

    mock_archive_finished_run.assert_called_once_with("run_dir", "archive_dir")

    # For not finished run
    mock_run.return_value.returncode = 1

    instrument_transfer.final_sync_to_storage(
        run_dir="run_dir",
        destination="destination",
        archive_dir="archive_dir",
        rsync_log="log_path",
    )

    assert mock_run.call_count == 3


def test_archive_finished_run():
    # Set up combinatorial testing for cases
    archive_dirs = [
        "/data/nosync",
        "/data/nosync/experiment/sample",
        "/data/nosync/experiment/sample/run",
    ]
    neighbor_dirs = [
        None,
        "/data/experiment/sample/run2",
        "/data/experiment/sample2",
    ]

    for archive_dir in archive_dirs:
        for neighbor_dir in neighbor_dirs:
            # Set up tmp dir
            tmp = tempfile.TemporaryDirectory()
            tmp_path = tmp.name

            # Create run dir
            experiment_path = tmp_path + "/data/experiment"
            sample_path = experiment_path + "/sample"
            run_path = sample_path + f"/{DUMMY_RUN_NAME}"
            os.makedirs(run_path)

            # Create neighbor dir, if any
            if neighbor_dir:
                neighbor_path = tmp_path + neighbor_dir
                os.mkdir(neighbor_path)

            # Create archive dir
            archive_path = tmp_path + archive_dir
            os.makedirs(archive_path)

            # Execute code
            instrument_transfer.archive_finished_run(run_path, archive_path)

            # Assert run is moved to archive dir
            assert os.path.exists(archive_path + f"/experiment/sample/{DUMMY_RUN_NAME}")

            # Assert run is removed from original location
            assert not os.path.exists(run_path)

            # Assert experiment and sample dirs are removed if empty
            if neighbor_dir:
                assert os.path.exists(neighbor_path)
                if neighbor_dir == "/data/experiment/sample/run2":
                    assert os.path.exists(sample_path)
                else:
                    assert not os.path.exists(sample_path)
            else:
                assert not os.path.exists(experiment_path)

            tmp.cleanup()


def test_parse_position_logs(setup_test_fixture):
    # Run fixture
    args, tmp, file_paths = setup_test_fixture

    logs = instrument_transfer.parse_position_logs(args.minknow_logs_dir)

    # Check length
    assert len(logs) == 24

    # Check uniqueness
    logs_as_strings = [json.dumps(log, sort_keys=True) for log in logs]
    assert len(logs) == len(set(logs_as_strings))

    for entry in logs:
        assert re.match(r"^(MN19414)|(1A)$", entry["position"])
        assert re.match(r"^2024-01-01 0\d:0\d:0\d.0\d$", entry["timestamp"])
        assert re.match(r"^INFO: [a-z\._]+ \(user_messages\)$", entry["category"])

        assert re.match(r"^(TEST12345)|(PAM12345)$", entry["body"]["flow_cell_id"])
        assert re.match(r"^\d+$", entry["body"]["num_pores"])
        assert re.match(r"^\d+$", entry["body"]["total_pores"])


def test_get_pore_counts(setup_test_fixture):
    # Run fixture
    args, tmp, file_paths = setup_test_fixture

    logs = instrument_transfer.parse_position_logs(args.minknow_logs_dir)
    pore_counts = instrument_transfer.get_pore_counts(logs)

    # Check length
    assert len(pore_counts) == 16

    # Check uniqueness
    pore_counts_as_strings = [json.dumps(log, sort_keys=True) for log in logs]
    assert len(logs) == len(set(pore_counts_as_strings))

    for entry in pore_counts:
        assert re.match(r"^(TEST12345)|(PAM12345)$", entry["flow_cell_id"])
        assert re.match(r"^(MN19414)|(1A)$", entry["position"])
        assert re.match(r"^2024-01-01 0\d:0\d:0\d.0\d$", entry["timestamp"])

        assert re.match(r"^(qc)|(mux)$", entry["type"])
        assert re.match(r"^\d+$", entry["num_pores"])
        assert re.match(r"^\d+$", entry["total_pores"])


def test_dump_pore_count_history(setup_test_fixture):
    # Run fixture
    args, tmp, file_paths = setup_test_fixture

    logs = instrument_transfer.parse_position_logs(args.minknow_logs_dir)
    pore_counts = instrument_transfer.get_pore_counts(logs)

    # Nothing to add, no file
    tmp = tempfile.TemporaryDirectory()
    run_path = tmp.name + f"/experiment/sample/{DUMMY_RUN_NAME.replace('TEST','FLG')}"
    os.makedirs(run_path)
    new_file = instrument_transfer.dump_pore_count_history(run_path, pore_counts)
    assert open(new_file).read() == ""
    tmp.cleanup()

    # Nothing to add, file is present
    tmp = tempfile.TemporaryDirectory()
    run_path = tmp.name + f"/experiment/sample/{DUMMY_RUN_NAME.replace('TEST','FLG')}"
    os.makedirs(run_path)
    open(run_path + "/pore_count_history.csv", "w").write("test")
    new_file = instrument_transfer.dump_pore_count_history(run_path, pore_counts)
    assert open(new_file).read() == "test"
    tmp.cleanup()

    # Something to add
    tmp = tempfile.TemporaryDirectory()
    run_path = tmp.name + f"/experiment/sample/{DUMMY_RUN_NAME}"
    os.makedirs(run_path)
    new_file = instrument_transfer.dump_pore_count_history(run_path, pore_counts)

    template = (
        "\n".join(
            [
                "flow_cell_id,timestamp,position,type,num_pores,total_pores",
                "TEST12345,2024-01-01 01:01:01.01,MN19414,qc,1111,1111",
                "TEST12345,2024-01-01 01:01:01.00,MN19414,mux,1110,1110",
                "TEST12345,2024-01-01 01:00:01.01,MN19414,qc,1011,1011",
                "TEST12345,2024-01-01 01:00:01.00,MN19414,mux,1010,1010",
                "TEST12345,2024-01-01 00:01:01.01,1A,qc,0111,0111",
                "TEST12345,2024-01-01 00:01:01.00,1A,mux,0110,0110",
                "TEST12345,2024-01-01 00:00:01.01,1A,qc,0011,0011",
                "TEST12345,2024-01-01 00:00:01.00,1A,mux,0010,0010",
            ]
        )
        + "\n"
    )

    assert open(new_file).read() == template
    tmp.cleanup()
