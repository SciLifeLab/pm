import importlib
import os
from unittest.mock import patch

import yaml

from taca.nanopore import ONT_run_classes

# To check coverage, use
# pytest -s --cov=taca.nanopore.ONT_run_classes --cov-report term-missing -vv tests/pytest/test_ONT_run_classes.py


def make_test_config(tmp):
    test_config_yaml_string = f"""mail: 
    recipients: mock
statusdb: mock
nanopore_analysis:
    run_types:
        user_run:
            data_dirs:
                - {tmp.name}/sequencing/promethion
                - {tmp.name}/sequencing/minion
            ignore_dirs:
                - 'nosync'
                - 'qc'
            instruments:
                promethion:
                    transfer_log: {tmp.name}/log/transfer_promethion.tsv
                    archive_dir: {tmp.name}/sequencing/promethion/nosync
                    metadata_dir: {tmp.name}/ngi-nas-ns/promethion_data
                    destination: {tmp.name}/miarka/promethion/
                minion:
                    transfer_log: /{tmp.name}/log/transfer_minion.tsv
                    archive_dir: {tmp.name}/sequencing/minion/nosync
                    metadata_dir: {tmp.name}/ngi-nas-ns/minion_data
                    destination: {tmp.name}/miarka/minion/
        qc_run:
            data_dirs:
                - {tmp.name}/sequencing/minion/qc
            ignore_dirs:
                - 'nosync'
            instruments:
                minion:
                    transfer_log: {tmp.name}/log/transfer_minion_qc.tsv
                    archive_dir: {tmp.name}/sequencing/minion/qc/nosync
                    metadata_dir: {tmp.name}/ngi-nas-ns/minion_data/qc
                    destination: {tmp.name}/miarka/minion/qc
                anglerfish:
                    anglerfish_samplesheets_dir: /srv/ngi-nas-ns/samplesheets/anglerfish
                    anglerfish_path: ~/miniconda3/envs/anglerfish/bin/anglerfish
    minknow_reports_dir: {tmp.name}/minknow_reports/
    rsync_options:
        '-Lav': None
        '--chown': ':ngi2016003'
        '--chmod': 'Dg+s,g+rw'
        '-r': None
        '--exclude': ['work']"""

    test_config_yaml = yaml.safe_load(test_config_yaml_string)

    return test_config_yaml


def write_pore_count_history(
    run_path,
    flowcell_id="TEST12345",
    instrument_position="1A",
):
    lines = [
        "flow_cell_id,timestamp,position,type,num_pores,total_pores",
        f"{flowcell_id},2024-01-24 12:00:39.757935,{instrument_position},qc,6753,6753",
        f"{flowcell_id},2024-01-23 11:00:39.757935,{instrument_position},mux,8000,8000",
    ]

    with open(run_path + "/pore_count_history.csv", "w") as f:
        for line in lines:
            f.write(line + "\n")


def create_run_dir(
    tmp,
    instrument="promethion",
    instrument_position="1A",
    flowcell_id="TEST12345",
    data_dir=None,
    experiment_name="experiment_name",
    sample_name="sample_name",
    script_files=False,
    run_finished=False,
    sync_finished=False,
):
    """Create a run directory according to specifications.

    ..
    └── {data_dir}
        └── 20240131_1702_{instrument_position}_{flowcell_id}_randomhash
            ├── run_path.txt
            └── pore_count_history.csv

    Return it's path.
    """
    if not data_dir:
        data_dir = f"{tmp.name}/sequencing/{instrument}"

    run_name = f"20240131_1702_{instrument_position}_{flowcell_id}_randomhash"
    run_path = f"{data_dir}/{run_name}"
    os.mkdir(run_path)

    # Add files conditionally
    if script_files:
        with open(run_path + "/run_path.txt", "w") as f:
            f.write(f"{experiment_name}/{sample_name}/{run_name}")
        write_pore_count_history(run_path, flowcell_id, instrument_position)

    if run_finished:
        open(f"{run_path}/final_summary_{run_name}.txt", "w").close()
        open(f"{run_path}/report_{run_name}.html", "w").close()
        open(f"{run_path}/report_{run_name}.json", "w").close()
        open(f"{run_path}/pore_activity_{run_name}.csv", "w").close()

    if sync_finished:
        open(f"{run_path}/.sync_finished", "w").close()

    return run_path


def test_ONT_user_run(create_dirs):
    """This test instantiates an ONT_user_run object and checks that the run_abspath attribute is set correctly."""

    # Create dir tree
    tmp = create_dirs

    # Mock db
    mock_db = patch("taca.utils.statusdb.NanoporeRunsConnection")
    mock_db.start()

    # Mock CONFIG
    test_config_yaml = make_test_config(tmp)
    mock_config = patch("taca.utils.config.CONFIG", new=test_config_yaml)
    mock_config.start()

    # Create run dir
    run_path = create_run_dir(tmp)

    # Reload module to add mocks
    importlib.reload(ONT_run_classes)
    # Instantiate run object
    run = ONT_run_classes.ONT_user_run(run_path)

    assert run.run_abspath == run_path
