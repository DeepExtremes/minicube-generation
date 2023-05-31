import segmentation_models_pytorch as smp
import torch
from torch.utils.model_zoo import load_url
import xarray as xr

_CHECKPOINT_URL = "https://nextcloud.bgc-jena.mpg.de/s/Ti4aYdHe2m3jBHy/" \
                  "download/mobilenetv2_l2a_rgbnir.pth"
_CHECKPOINT_BANDS = ['B02', 'B03', "B04", "B8A"]
_CHECKPOINT_COORDS = ['time', 'band', 'y', 'x']
_SCALE_FACTOR = 2


def _prepare_source(ds: xr.Dataset) -> xr.Dataset:
    to_drop = []
    for data_var in ds.data_vars:
        if data_var not in _CHECKPOINT_BANDS:
            to_drop.append(data_var)
    for coord in ds.coords:
        if coord not in _CHECKPOINT_COORDS:
            to_drop.append(coord)
    ds_sub = ds.drop_vars(to_drop)
    return ds_sub


def compute_earthnet_cloudmask(ds_source: xr.Dataset) -> xr.Dataset:
    checkpoint = load_url(_CHECKPOINT_URL)
    model = smp.Unet(
        encoder_name="mobilenet_v2",
        encoder_weights=None,
        classes=4,
        in_channels=len(_CHECKPOINT_BANDS)
    )
    model.load_state_dict(checkpoint)
    model.eval()
    ds = _prepare_source(ds_source)
    da = ds.to_array(dim='band').fillna(1.0).transpose(_CHECKPOINT_COORDS)
    x = torch.from_numpy(da.values.astype("float32"))
    b, c, h, w = x.shape

    h_big = ((h // 32 + 1) * 32)
    h_pad_left = (h_big - h) // 2
    h_pad_right = ((h_big - h) + 1) // 2

    w_big = ((w // 32 + 1) * 32)
    w_pad_left = (w_big - w) // 2
    w_pad_right = ((w_big - w) + 1) // 2

    x = torch.nn.functional.pad(x, (
    w_pad_left, w_pad_right, h_pad_left, h_pad_right), mode="reflect")
    x = torch.nn.functional.interpolate(
        x, scale_factor=_SCALE_FACTOR, mode='bilinear'
    )
    with torch.no_grad():
        y_hat = model(x)
    y_hat = torch.argmax(y_hat, dim=1).float()
    y_hat = torch.nn.functional.max_pool2d(y_hat[:, None, ...], kernel_size=2)[
            :, 0, ...]
    y_hat = y_hat[:, h_pad_left:-h_pad_right, w_pad_left:-w_pad_right]

    res = xr.Dataset()
    res['mask'] = (("time", "y", "x"), y_hat.cpu().numpy().astype('uint8'))
    return res
