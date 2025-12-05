from collections import namedtuple
from pathlib import Path

from qtpy.QtGui import QIcon
from databroker import Broker
from prefect.deployments import run_deployment

from xicam.core import msg
from xicam.plugins.settingsplugin import ParameterSettingsPlugin
from xicam.gui.static import path

from xicam.Acquire.runengine import get_run_engine


db = Broker.namded('local')

Resource = namedtuple('Resource',['root_map', 'root', 'path'])

class SciCatSettingsPlugin(ParameterSettingsPlugin):
    """Settings plugin for logging information and parameterization.
    """

    def __init__(self):
        self.resources = []

        super(SciCatSettingsPlugin, self).__init__(
            QIcon(str(path("icons/scicat.png"))),
            'SciCat',
            [

            ],
        )

        get_run_engine().RE.subscribe(self.consumer)

    def consumer(self, name, doc):
        if name == 'start':
            self.resources = []
        elif name == 'resource':
            self.resources.append(Resource(db.root_map, doc['root'], doc['resource_path']))
        elif name == 'stop':
            self.scicat_ingest(self.resources)

    def scicat_ingest(self, resources:list[Resource]):
        for resource in resources:
            root = resource.root_map.get(resource.root)
            full_path = Path(root) / resource.path

            flow_run = run_deployment(
                name="dispatcher/run_7011_dispatcher",
                parameters={
                    "file_path": full_path,  # for example: "test/test_065.h5"
                    "is_export_control": False
                    # Not sure if you deal with confidential data that can't be stored on NERSC, but in that case, you can optionally set this to True. Otherwise, it defaults to False.
                }
            )