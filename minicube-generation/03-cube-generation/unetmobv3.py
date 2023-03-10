import dask.array as da
import gdown
import itertools
import numpy as np
import pathlib
from tqdm import tqdm
from typing import List
import xarray as xr

from maskay.tensorsat import TensorSat
from maskay.torch import Module
from maskay.utils import get_models_path


class UnetMobV3(Module):
    def __init__(self):
        super().__init__()
        self.model = model_setup()

    def forward(self, x):
        return self.model(x)

    def inProcessing(self, tensor: np.ndarray):
        # If all the pixels are zero skip the run and outProcessing.
        if da.sum(tensor) == 0:
            shp = tensor.shape
            tensor = da.zeros(
                (shp[0], 4, shp[2], shp[3])
            )  # 4 is the number of the output classes
            return [tensor]
        return tensor / 10000

    def outProcessing(self, tensor: np.ndarray):
        return (self.softmax(tensor, axis=1) * 10000).astype(np.int16)

    def softmax(self, X, theta=1.0, axis=None):
        """
        Compute the softmax of each element along an axis of X.

        Parameters
        ----------
        X: ND-Array. Probably should be floats.
        theta (optional): float parameter, used as a multiplier
            prior to exponentiation. Default = 1.0
        axis (optional): axis to compute values along. Default is the
            first non-singleton axis.

        Returns an array the same size as X. The result will sum to 1
        along the specified axis.
        """

        # make X at least 2d
        y = da.atleast_2d(X)

        # find axis
        if axis is None:
            axis = next(j[0] for j in enumerate(y.shape) if j[1] > 1)

        # multiply y against the theta parameter,
        y = y * float(theta)

        # subtract the max for numerical stability
        y = y - da.expand_dims(da.max(y, axis=axis), axis)

        # exponentiate y
        y = da.exp(y)

        # take the sum along the specified axis
        ax_sum = da.expand_dims(da.sum(y, axis=axis), axis)

        # finally: divide elementwise
        p = y / ax_sum

        # flatten if X was 1D
        if len(X.shape) == 1:
            p = p.flatten()

        return p

    def _Crop(self, tensor: xr.DataArray, cropsize: int, overlap: int):
        # Select the raster with the lowest resolution
        tshp = tensor.shape

        if len(tshp) == 2:
            return super()._Crop(tensor, cropsize, overlap)

        # if the image is too small, return (0, 0)
        # if (tshp[-2] < cropsize) and (tshp[-1] < cropsize):
        #     return [(0, 0)]

        # Define relative coordinates.
        xmn, xmx, ymn, ymx = (0, tshp[-2], 0, tshp[-1])

        if overlap > cropsize:
            raise ValueError("The overlap must be smaller than the cropsize")

        time_range= np.arange(0, tshp[0], 1)
        xrange = np.arange(xmn, xmx, (cropsize - overlap))
        yrange = np.arange(ymn, ymx, (cropsize - overlap))

        # If there is negative values in the range, change them by zero.
        xrange[xrange < 0] = 0
        yrange[yrange < 0] = 0

        # Remove the last element if it is outside the tensor
        xrange = xrange[xrange - (tshp[-2] - cropsize) <= 0]
        yrange = yrange[yrange - (tshp[-1] - cropsize) <= 0]

        # If the last element is not (tshp[1] - cropsize) add it!
        if xrange[-1] != (tshp[-2] - cropsize):
            xrange = np.append(xrange, tshp[-2] - cropsize)
        if yrange[-1] != (tshp[-1] - cropsize):
            yrange = np.append(yrange, tshp[-1] - cropsize)

        # Create all the relative coordinates
        mrs = list(itertools.product(time_range, xrange, yrange))

        return mrs

    def _predict(self, tensor: TensorSat):
        # Obtain the zero coordinate to create an IP
        zero_coord = self._MagickCrop(tensor)

        # Number of image patches (IPs)
        IPslen = self.get_ips(zero_coord)

        # Get the cropsize for each raster
        tensor_cropsize = self.get_cropsize(tensor)

        # Raster ref (lowest resolution)
        rbase = tensor.rasterbase()
        rbase_name = tensor.rasterbase_name()

        # Create outensor
        # outensor = None
        # outensor_ds = None
        out_ds = xr.Dataset(coords = rbase.coords)

        batch_iter = range(0, IPslen, self.batchsize)
        for index in tqdm(batch_iter, disable=self.quiet):
            batched_IP = list()
            zerocoords = list()
            for index2 in range(index,
                                (index + self.batchsize)):
                # Container to create a full IP with all bands with the same resolution
                IP = list()

                # Reference raster IP
                bmrt, bmrx, bmry = zero_coord[rbase_name][index2]
                rbase_ip = rbase[
                           bmrt: (bmrt + 1),
                           bmrx: (bmrx + self.cropsize),
                           bmry: (bmry + self.cropsize)
                           ]
                base_cropsize = tensor_cropsize[rbase_name]

                for key, _ in zero_coord.items():
                    # Select the zero coordinate
                    mrt, mrx, mry = zero_coord[key][index2]

                    # Obtain the specific cropsize for each raster
                    cropsize = tensor_cropsize[key]

                    # Crop the full raster using specific coordinates
                    tensorIP = tensor.dictarray[key][
                               mrt: (mrt + 1),
                               mrx: (mrx + cropsize),
                               mry: (mry + cropsize)
                               ].squeeze()

                    # Resample the raster to the reference raster
                    if base_cropsize != cropsize:
                        tensorIP = self._align(rbase_ip, tensorIP)

                        # Append the IP to the container
                    IP.append(tensorIP)

                # Stack the IP
                IP = da.stack(IP, axis=0)

                # Append the IP to the batch
                zerocoords.append((bmrt, bmrx, bmry))
                batched_IP.append(IP)

            # Stack the batch
            batched_IP = da.stack(batched_IP, axis=0)

            # Change order of the batched_IP
            if self.order == "BHWC":
                batched_IP = da.moveaxis(batched_IP, 1, -1)

            # Run the preprocessing
            batched_IP = self.inProcessing(batched_IP)

            # Run the model
            batched_IP = self._run(batched_IP.compute())

            # Run the postprocessing
            batched_IP = self.outProcessing(batched_IP)

            # If is the first iteration, create the output tensor
            # if outensor is None:
                # Create the output tensor rio xarray object
                # timecoord = rbase.coords["time"][0]
                # xcoord = rbase.coords["y"]
                # ycoord = rbase.coords["x"]
                # coords = [timecoord, xcoord, ycoord]


                # classes = batched_IP.shape[1]
                # dtype = batched_IP.dtype
                # outensor = da.zeros(
                #     shape=(rbase.shape[0],
                #            classes,
                #            rbase.shape[1],
                #            rbase.shape[2]),
                #     dtype=dtype
                # )

            # Copy the IP values in the outputtensor
            gather_zerocoord = self._MagickGather(out_ds, zerocoords)

            for index3, zcoords in enumerate(gather_zerocoord):
                # Coordinates to copy the IP
                Tmin, (Xmin, Ymin), (Xmax, Ymax) = zcoords["outensor"]
                (XIPmin, YIPmin), (XIPmax, YIPmax) = zcoords["ip"]

                # Copy the IP
                # outensor[Tmin, :, Xmin:Xmax, Ymin:Ymax] = batched_IP[
                #                                     index3, :, XIPmin:XIPmax,
                #                                     YIPmin:YIPmax
                #                                     ]
                # for i in range(4):
                x_coords = rbase.coords['x'][Ymin:Ymax]
                y_coords = rbase.coords['y'][Xmin:Xmax]
                time_coords = rbase.coords['time'][Tmin].expand_dims({'time': 1})
                coords = {
                    'time': time_coords,
                    'y': y_coords,
                    'x': x_coords
                }
                cloud_clear = xr.DataArray(
                    batched_IP[index3, 0, XIPmin:XIPmax, YIPmin:YIPmax].reshape(1, cropsize, cropsize),
                    # name='Cloud Clear Probability',
                    name='cloud_clear',
                    coords=coords,
                    dims=list(coords.keys())
                )
                cloud_thick_prob = xr.DataArray(
                    batched_IP[index3, 1, XIPmin:XIPmax, YIPmin:YIPmax].reshape(1, cropsize, cropsize),
                    # name='Cloud Thickness Probability',
                    name='cloud_thick_prob',
                    coords=coords,
                    dims=list(coords.keys())
                )
                cloud_thin_prob = xr.DataArray(
                    batched_IP[index3, 2, XIPmin:XIPmax, YIPmin:YIPmax].reshape(1, cropsize, cropsize),
                    # name='Cloud Thinness Probability',
                    name='cloud_thin_prob',
                    coords=coords,
                    dims=list(coords.keys())
                )
                cloud_shadow_prob = xr.DataArray(
                    batched_IP[index3, 3, XIPmin:XIPmax, YIPmin:YIPmax].reshape(1, cropsize, cropsize),
                    # name='Cloud Shadow Probability',
                    name='cloud_shadow_prob',
                    coords=coords,
                    dims=list(coords.keys())
                )
                # out_ds = xr.merge([cloud_clear, cloud_thick_prob, cloud_thin_prob, cloud_shadow_prob])
                # if outensor_ds is None:
                #     outensor_ds = out_ds
                # else:
                out_ds = xr.merge([out_ds, cloud_clear, cloud_thick_prob, cloud_thin_prob, cloud_shadow_prob])

        # Create the output tensor rio xarray object
        # timecoord= rbase.coords["time"].values
        # xcoord = rbase.coords["y"].values
        # ycoord = rbase.coords["x"].values
        # coords = [timecoord, np.arange(0, classes), xcoord, ycoord]
        # dims = ["time", "band", "y", "x"]
        #
        # return (
        #     xr.DataArray(outensor, coords=coords, dims=dims)
        #         .rio.write_nodata(-999)
        #         .rio.write_transform(rbase.rio.transform())
        # )
        return out_ds

    # def _MagickGather(self, outensor: np.ndarray, batch_step: List):
    def _MagickGather(self, outensor: xr.Dataset, batch_step: List):
        """Gather the image patches and merge them into a single tensor.

        Args:
            tensorprob (np.ndarray): A tensor with shape (B, C, H, W).
            outensor (np.ndarray): A tensor with shape (C, H, W).
            quiet (bool, optional): If True, do not print any messages. Defaults to False.

        Returns:
            np.ndarray: A np.ndarray with shape (C, H, W) with the image patches.
        """

        # Get CropMagick properties
        xmin, xmax = (0, outensor['x'].shape)  # X borders of the output tensor
        ymin, ymax = (0, outensor['y'].shape)  # Y borders of the output tensor

        container = list()
        for coord in batch_step:
            #  Define Time dimension pixels
            Tmin = coord[0]

            #  Define X dimension pixels
            if coord[1] == xmin:
                Xmin = coord[1]
                XIPmin = 0
            else:
                Xmin = coord[1] + self.overlap // 2
                XIPmin = self.overlap // 2

            if (coord[1] + self.cropsize) == xmax:
                Xmax = coord[1] + self.cropsize
                XIPmax = self.cropsize
            else:
                Xmax = coord[1] + self.cropsize - self.overlap // 2
                XIPmax = self.cropsize - self.overlap // 2

            #  Define Y dimension pixels
            if coord[2] == ymin:
                Ymin = coord[2]
                YIPmin = 0
            else:
                Ymin = coord[2] + self.overlap // 2
                YIPmin = self.overlap // 2

            if (coord[2] + self.cropsize) == ymax:
                Ymax = coord[2] + self.cropsize
                YIPmax = self.cropsize
            else:
                Ymax = coord[2] + self.cropsize - self.overlap // 2
                YIPmax = self.cropsize - self.overlap // 2

            # Put the IP tensor in the output tensor
            container.append(
                {
                    "outensor": [Tmin, (Xmin, Ymin), (Xmax, Ymax)],
                    "ip": [(XIPmin, YIPmin), (XIPmax, YIPmax)],
                }
            )
        return container


def model_setup():
    # Check if packages are installed
    is_external_package_installed = []

    try:
        import pytorch_lightning as pl
    except ImportError:
        is_external_package_installed.append("pytorch_lightning")

    try:
        import segmentation_models_pytorch as smp
    except ImportError:
        is_external_package_installed.append("segmentation_models_pytorch")

    if is_external_package_installed != []:
        nopkgs = ', '.join(is_external_package_installed)
        raise ImportError(
            f"Please install the following packages: {nopkgs}."
        )

    class UnetMobV2Class(pl.LightningModule):
        def __init__(self):
            super().__init__()
            self.model = smp.Unet(
                encoder_name="mobilenet_v2",
                encoder_weights=None,
                in_channels=13,
                classes=4,
            )

        def forward(self, x):
            return self.model(x)

    filename = pathlib.Path(get_models_path()) / "unetmobv2.ckpt"
    # Download the model if it doesn't exist
    if not filename.is_file():
        # download file using gdown
        url = "https://drive.google.com/uc?id=1o9LeVsXCeD2jmS-G8s7ZISfciaP9v-DU"
        gdown.download(url, filename.as_posix())
    # Load the model
    model = UnetMobV2Class().load_from_checkpoint(filename.as_posix())
    model.eval()
    return model
