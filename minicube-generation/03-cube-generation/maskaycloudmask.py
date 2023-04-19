import maskay
import xarray as xr

from unetmobv3 import UnetMobV3

_CLOUD_MASK_NAMES = [
    'cloud_clear_prob', 'cloud_thick_prob',
    'cloud_thin_prob', 'cloud_shadow_prob'
]


def compute_cloud_mask(ds_source: xr.Dataset,
                       device: str = 'cpu',
                       batch_size: int = 15) -> xr.Dataset:
    model = UnetMobV3()
    predictor = maskay.Predictor(
        cropsize=128,
        overlap=0,
        device=device,
        quiet=False,
        batchsize=batch_size
    )
    tensor = maskay.TensorSat(
        Aerosol=ds_source.B01 * 10000,
        Blue=ds_source.B02 * 10000,
        Green=ds_source.B03 * 10000,
        Red=ds_source.B04 * 10000,
        RedEdge1=ds_source.B05 * 10000,
        RedEdge2=ds_source.B06 * 10000,
        RedEdge3=ds_source.B07 * 10000,
        NIR=ds_source.B08 * 10000,
        NIR2=ds_source.B8A * 10000,
        WaterVapor=ds_source.B09 * 10000,
        Cirrus=ds_source.B10 * 10000,
        SWIR1=ds_source.B11 * 10000,
        SWIR2=ds_source.B12 * 10000,
        cache=True,
        align=False
    )

    cm_ds = predictor.predict(model, tensor)
    cm_ds = cm_ds.assign(crs=ds_source.crs)

    return cm_ds
