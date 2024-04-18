"""Classes for handling connection to StatusDB."""

import csv
import logging
from datetime import datetime

import couchdb

logger = logging.getLogger(__name__)


class StatusdbSession:
    """Wrapper class for couchdb."""

    def __init__(self, config, db=None):
        user = config.get("username")
        password = config.get("password")
        url = config.get("url")
        url_string = f"https://{user}:{password}@{url}"
        display_url_string = "https://{}:{}@{}".format(user, "*********", url)
        self.connection = couchdb.Server(url=url_string)
        if not self.connection:
            raise Exception(f"Couchdb connection failed for url {display_url_string}")
        if db:
            self.db_connection = self.connection[db]

    def get_entry(self, name, use_id_view=False):
        """Retrieve entry from a given db for a given name.

        :param name: unique name identifier (primary key, not the uuid)
        """
        if use_id_view:
            view = self.id_view
        else:
            view = self.name_view
        if not view.get(name, None):
            return None
        return self.db.get(view.get(name))

    def save_db_doc(self, doc, db=None):
        try:
            db = db or self.db
            db.save(doc)
        except Exception as e:
            raise Exception(f"Failed saving document due to {e}")

    def get_project_flowcell(
        self, project_id, open_date="2015-01-01", date_format="%Y-%m-%d"
    ):
        """From information available in flowcell db connection,
        collect the flowcell this project was sequenced.

        :param project_id: NGI project ID to get the flowcells
        :param open_date: Open date of project to skip the check for all flowcells
        :param date_format: The format of specified open_date
        """
        try:
            open_date = datetime.strptime(open_date, date_format)
        except:
            open_date = datetime.strptime("2015-01-01", "%Y-%m-%d")

        project_flowcells = {}
        date_sorted_fcs = sorted(
            list(self.proj_list.keys()),
            key=lambda k: datetime.strptime(k.split("_")[0], "%y%m%d"),
            reverse=True,
        )
        for fc in date_sorted_fcs:
            fc_date, fc_name = fc.split("_")
            if datetime.strptime(fc_date, "%y%m%d") < open_date:
                break
            if (
                project_id in self.proj_list[fc]
                and fc_name not in project_flowcells.keys()
            ):
                project_flowcells[fc_name] = {
                    "name": fc_name,
                    "run_name": fc,
                    "date": fc_date,
                    "db": self.db.name,
                }
        return project_flowcells


class ProjectSummaryConnection(StatusdbSession):
    def __init__(self, config):
        super().__init__(config)
        self.projects_db = self.connection["projects"]
        self.name_view = {
            k.key: k.id
            for k in self.projects_db.view("project/project_name", reduce=False)
        }
        self.id_view = {
            k.key: k.id
            for k in self.projects_db.view("project/project_id", reduce=False)
        }


class FlowcellRunMetricsConnection(StatusdbSession):
    def __init__(self, config):
        super().__init__(config)
        self.flowcells_db = self.connection["flowcells"]
        self.name_view = {k.key: k.id for k in self.db.view("names/name", reduce=False)}
        self.proj_list = {
            k.key: k.value
            for k in self.flowcells_db.view("names/project_ids_list", reduce=False)
            if k.key
        }


class X_FlowcellRunMetricsConnection(StatusdbSession):
    def __init__(self, config, dbname="x_flowcells"):
        super().__init__(config)
        self.x_flowcells_db = self.connection["x_flowcells"]
        self.name_view = {
            k.key: k.id for k in self.x_flowcells_db.view("names/name", reduce=False)
        }
        self.proj_list = {
            k.key: k.value
            for k in self.x_flowcells_db.view("names/project_ids_list", reduce=False)
            if k.key
        }


class NanoporeRunsConnection(StatusdbSession):
    def __init__(self, config):
        super().__init__(config)
        self.nanopore_runs_db = self.connection["nanopore_runs"]

    def check_run_exists(self, ont_run) -> bool:
        view_names = self.nanopore_runs_db.view("names/name")
        if len(view_names[ont_run.run_name].rows) > 0:
            return True
        else:
            return False

    def check_run_status(self, ont_run) -> str:
        view_all_stats = self.nanopore_runs_db.view("names/name")
        doc_id = view_all_stats[ont_run.run_name].rows[0].id
        return self.nanopore_runs_db[doc_id]["run_status"]

    def create_ongoing_run(
        self, ont_run, run_path_file: str, pore_count_history_file: str
    ):
        run_path = open(run_path_file).read().strip()

        pore_counts = []
        with open(pore_count_history_file) as stream:
            for line in csv.DictReader(stream):
                pore_counts.append(line)

        new_doc = {
            "run_path": run_path,
            "run_status": "ongoing",
            "pore_count_history": pore_counts,
        }

        new_doc_id, new_doc_rev = self.nanopore_runs_db.save(new_doc)
        logger.info(
            f"New database entry created: {ont_run.run_name}, id {new_doc_id}, rev {new_doc_rev}"
        )

    def finish_ongoing_run(self, ont_run, dict_json: dict):
        view_names = self.nanopore_runs_db.view("names/name")
        doc_id = view_names[ont_run.run_name].rows[0].id
        doc = self.nanopore_runs_db[doc_id]

        doc.update(dict_json)
        doc["run_status"] = "finished"
        self.nanopore_runs_db[doc.id] = doc


def update_doc(db, obj, over_write_db_entry=False):
    view = db.view("info/name")
    if len(view[obj["name"]].rows) == 1:
        remote_doc = view[obj["name"]].rows[0].value
        doc_id = remote_doc.pop("_id")
        doc_rev = remote_doc.pop("_rev")
        if remote_doc != obj:
            if not over_write_db_entry:
                obj = merge_dicts(obj, remote_doc)
            obj["_id"] = doc_id
            obj["_rev"] = doc_rev
            db[doc_id] = obj
            logger.info("Updating {}".format(obj["name"]))
    elif len(view[obj["name"]].rows) == 0:
        db.save(obj)
        logger.info("Saving {}".format(obj["name"]))
    else:
        logger.warn("More than one row with name {} found".format(obj["name"]))


def merge_dicts(d1, d2):
    """Merge dictionary d2 into dictionary d1.
    If the same key is found, the one in d1 will be used.
    """
    for key in d2:
        if key in d1:
            if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                merge_dicts(d1[key], d2[key])
            elif d1[key] == d2[key]:
                pass  # same leaf value
            else:
                logger.debug(
                    f"Values for key {key} in d1 and d2 differ, "
                    "using the value of d1"
                )
        else:
            d1[key] = d2[key]
    return d1
