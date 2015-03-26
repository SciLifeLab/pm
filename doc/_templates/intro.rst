TACA code structure
===================

TACA uses `click`_ for building a structured CLI application. So for further details,
please refer to click documentation.

The structure that we have decided to follow for this repository is to create a
``cli.py`` file on each subcommand (submodule) in order to better isolate CLI methods
and thus ease the maintenance. In order to add a new subcommand you should also
create a new ``entry_point`` in ``setup.py``. If you have further questions, don't
hesitate on opening an issue and tag it with the ``question`` tag.

TACA configuration
==================

Mainly, TACA requires a `YAML`_ configuration file to function. This is required,
and whenever you invoke TACA it will look for it. The order is:

1. ``-c | --config`` command line option
2. Environment variable ``TACA_CONFIG``
3. ~/.taca/taca.yaml

If none of these are found, TACA will exit with an error message.

This is how the configuration file should look like:

.. code-block:: yaml

    log:
        log_file: /path/where/to/store/the/log/file
    storage:
        data_dirs:
            - list of
            - runs directories
        archive_dirs:
            - list of old runs directories
            - to be long term archived
        irods:
            irodsHome: Path to irods archiving directory

    preprocessing:
        hiseq_data_dir: /path/to/hiseq/data
        miseq_dat_dir: /path/to/miseq/data
        # MFS server to put metadata in
        mfs: /path/to/mfs/partition
        # Directory where to find status files for transfers and analysis
        status_dir: /path/to/status_dir
        # Location of samplesheets for demultiplexing
        samplesheets_dir: /path/to/samplesheets/dir
        bcl2fastq:
            XTen: /path/to/bcl2fastq (XTen version)
            HiSeq: /path/to/bcl2fastq (HiSeq version)
            MiSeq: /path/to/bcl2fastq (MiSeq version)
            - all command line options of bcl2fastq , i.e runfolder, input-dir, etc.
        sync:
            user: remote_user_analysis_server
            host: analysis_server
            data_archive: /path/where/to/transfer/data
            include:
                - "files"
                - "to"
                - "include"
                - "in"
                - "the"
                - "transfer"
        analysis:
            host: analysis_server
            port: port
            url: url_to_start_flowcell_analysis

.. EXTERNAL LINKS

.. _click: http://click.pocoo.org/3/
.. _YAML: http://en.wikipedia.org/wiki/YAML
