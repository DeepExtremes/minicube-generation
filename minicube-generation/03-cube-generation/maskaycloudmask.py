import maskay
from maskay.library.unetmobv2 import UnetMobV2
import xarray as xr

_CLOUD_MASK_NAMES = [
    'cloud_clear_prob', 'cloud_thick_prob',
    'cloud_thin_prob', 'cloud_shadow_prob'
]


def _compute_mask_for_timestep(ds_slice: xr.Dataset,
                               model,
                               predictor: maskay.Predictor) -> xr.Dataset:
    print(f'Processing cloud mask for {ds_slice.time}')
    tensor = maskay.TensorSat(
        Aerosol=ds_slice.B01 * 10000,
        Blue=ds_slice.B02 * 10000,
        Green=ds_slice.B03 * 10000,
        Red=ds_slice.B04 * 10000,
        RedEdge1=ds_slice.B05 * 10000,
        RedEdge2=ds_slice.B06 * 10000,
        RedEdge3=ds_slice.B07 * 10000,
        NIR=ds_slice.B08 * 10000,
        NIR2=ds_slice.B8A * 10000,
        WaterVapor=ds_slice.B09 * 10000,
        Cirrus=ds_slice.B10 * 10000,
        SWIR1=ds_slice.B11 * 10000,
        SWIR2=ds_slice.B12 * 10000,
        cache=True,
        align=False
    )
    cm_ds = predictor.predict(model, tensor).to_dataset(dim='band')
    data_arrays = []
    for index in range(len(_CLOUD_MASK_NAMES)):
        data_arrays.append(cm_ds[index].expand_dims(dim={
            'time': ds_slice.expand_dims(dim='time').time})
                           .rename(_CLOUD_MASK_NAMES[index]))
    return xr.combine_by_coords(data_arrays)


def compute_cloud_mask(ds_source: xr.Dataset) -> xr.Dataset:
    model = UnetMobV2()
    predictor = maskay.Predictor(
        cropsize=128,
        overlap=0,
        device="cpu",
        quiet=False
    )

    ds_target = _compute_mask_for_timestep(
        ds_source.isel(time=0), model, predictor
    )
    if len(ds_source.time) > 1:
        for ti in range(1, len(ds_source.time)):
            ds_slice = ds_source.isel(time=ti)
            ds_time_step = _compute_mask_for_timestep(
                ds_slice, model, predictor
            )
            ds_target = xr.concat([ds_target, ds_time_step], dim='time')
    return ds_target
