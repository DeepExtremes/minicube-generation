import inspect
import os
import sys
import unittest
import xarray as xr

from xcube.core.store import new_data_store

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
cubegen_dir = os.path.join(base_dir, 'minicube-generation', '03-cube-generation')
sys.path.insert(0, cubegen_dir)
from climatology import create_climatology
# from ......\deepextremesminicube-generation/03-cube-generation/climatology import create_climatology
# from .....m03-cube-generation.climatology import create_climatology

HAS_SH_CREDENTIALS = 'SH_CLIENT_ID' in os.environ \
                     and 'SH_CLIENT_SECRET' in os.environ
REQUIRE_SH_CREDENTIALS = 'requires SH credentials'

crs = "EPSG:32629"
L57_VARIABLE_NAMES = ["B02", "B03", "B04", "B05", "SR_CLOUD_QA"]
L5_TIME_RANGE = ["1982-07-16", "2013-06-05"]
L_PARAMS = dict(
    L5=dict(
        ID='L5',
        VARIABLE_NAMES=["B02", "B03", "B04", "B05", "SR_CLOUD_QA"],
        TIME_RANGE = ["1982-07-16", "2013-06-05"]
    ),
    L7=dict(
        ID='L7',
        VARIABLE_NAMES=["B02", "B03", "B04", "B05", "SR_CLOUD_QA"],
        TIME_RANGE=["1999-04-15", "2020-12-31"]
    ),
    L8=dict(
        ID='L8',
        VARIABLE_NAMES=["B02", "B03", "B04", "B05", "BQA"],
        TIME_RANGE=["2013-07-01", "2020-12-31"]
    ),
)


@unittest.skipUnless(HAS_SH_CREDENTIALS, REQUIRE_SH_CREDENTIALS)
class ClimatologyTest(unittest.TestCase):

    @classmethod
    def setUp(self) -> None:
        self.sh_store = new_data_store(
            'sentinelhub', num_retries=400,
            api_url='https://services-uswest2.sentinel-hub.com'
        )

    def test_create_climatology(self):
        # l5_ds = self._get_dataset_from_store('L5')
        l5_ds = None
        create_climatology(l5_ds)
        # sh_store = new_data_store(
        #     'sentinelhub', num_retries=400,
        #     api_url='https://services-uswest2.sentinel-hub.com'
        # )
        #
        # l5_ds = sh_store.open_data(
        #     "LTML2", variable_names=L57_VARIABLE_NAMES,
        #                            bbox=bbox, spatial_res=spatial_res, crs=crs,
        #                            time_range=L5_TIME_RANGE,
        #                            downsampling='NEAREST')


    def _get_dataset_from_store(self, code: str) -> xr.Dataset:
        x = 816149.4858603566
        y = 2915658.2750919457
        spatial_res = 20 * 2
        half_spatial_res = spatial_res / 2
        bbox = [x - half_spatial_res, y - half_spatial_res, x + half_spatial_res,
                y + half_spatial_res]
        crs = "EPSG:32629"
        params = L_PARAMS.get(code)
        return self.sh_store.open_data(
            params.get('ID'),
            variable_names=params.get('VARIABLE_NAMES'),
            time_rane=params.get('TIME_RANGE'),
            spatial_res=spatial_res,
            bbox=bbox,
            crs=crs
        )
