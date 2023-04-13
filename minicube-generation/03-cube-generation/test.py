import maskay
from unetmobv3 import UnetMobV3

from xcube.core.store import new_data_store

sh_store = new_data_store(
    "sentinelhub"
)
ds = sh_store.open_data('S2L1C',
                        bbox=[800085.9664725274, 3733342.2256813217, 802645.9664725274, 3735902.2256813217],
                        time_range=["2018-02-01", "2018-02-14"],
                        time_period='5D',
                        crs="EPSG:32629",
                        spatial_res = 20
                        )

tensor = maskay.TensorSat(
    Aerosol=ds.B01 * 10000,
    Blue=ds.B02 * 10000,
    Green=ds.B03 * 10000,
    Red=ds.B04 * 10000,
    RedEdge1=ds.B05 * 10000,
    RedEdge2=ds.B06 * 10000,
    RedEdge3=ds.B07 * 10000,
    NIR=ds.B08 * 10000,
    NIR2=ds.B8A * 10000,
    WaterVapor=ds.B09 * 10000,
    Cirrus=ds.B10 * 10000,
    SWIR1=ds.B11 * 10000,
    SWIR2=ds.B12 * 10000,
    cache=True,
    align=False
)

# Make a prediction
model = UnetMobV3()

predictor = maskay.Predictor(
    cropsize = 128,
    overlap = 0,
    device = "cpu",
    batchsize = 3,
    quiet = False,
)
result = predictor.predict(model, tensor)
result = result.assign(crs=ds.crs)
result.to_zarr('outensor3_new.zarr')
