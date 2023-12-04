import os
import re
from io import StringIO
from pathlib import Path
import ftplib
import fnmatch
from io import BytesIO
from typing import Generator, Tuple, Optional, Union, Callable, List

from tqdm import tqdm
import requests
import pandas as pd
import numpy as np


class DwdGlobalCLIMAT:

    STATIONS_URL = "https://opendata.dwd.de/climate_environment/CDC/help/stations_list_CLIMAT_data.txt"
    CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "cache" / "dwd-global"

    class MonthlyType:
        air_temperature_mean = "air_temperature_mean"
        air_temperature_absolute_max = "air_temperature_absolute_max"
        air_temperature_absolute_min = "air_temperature_absolute_min"
        air_temperature_mean_of_daily_max = "air_temperature_mean_of_daily_max"
        air_temperature_mean_of_daily_min = "air_temperature_mean_of_daily_min"
        mean_sea_level_pressure = "mean_sea_level_pressure"
        precipGE1mm_days = "precipGE1mm_days"
        precipitation_total = "precipitation_total"
        sunshine_duration = "sunshine_duration"
        vapour_pressure = "vapour_pressure"

    def __init__(
            self,
            force_ftp: bool = False,
            verbose: bool = True,
    ):
        self.force_ftp = force_ftp
        self.verbose = verbose
        self.stations = self._load_stations()

    def monthly_dataframe(
            self,
            type: str = MonthlyType.air_temperature_mean,
            station_info: bool = False,
    ) -> pd.DataFrame:
        return self._combined_dataframe(
            directory=f"climate_environment/CDC/observations_global/CLIMAT/monthly/qc/{type}/historical/",
            station_info=station_info,
        )

    def _combined_dataframe(
            self,
            directory: str,
            station_info: bool = False,
    ) -> pd.DataFrame:

        regex = re.compile(r"(\d+)_(\d{4})(\d{2})_(\d{4})(\d{2})")

        def _reduce_filenames(filenames: List[str]) -> List[str]:
            fn_map = {}
            for fn in filenames:
                match = regex.match(fn)
                if not match:
                    continue
                match = tuple(match.groups())

                key = match[:3]  # station-year-month
                if key not in fn_map:
                    fn_map[key] = {"filename": fn, "end": match[3:]}
                else:
                    if fn_map[key]["end"] < match[3:]:
                        fn_map[key] = {"filename": fn, "end": match[3:]}

            return sorted(
                v["filename"]
                for v in fn_map.values()
            )

        dfs = []
        for filename, df in self._iter_ftp_dataframes(
                directory=directory,
                pattern="*.txt",
                filenames_reduction=_reduce_filenames,
        ):
            match = regex.match(filename).groups()

            df["station_id"] = match[0]
            dfs.append(df)

        df = pd.concat(dfs)
        df["Jahr"] = pd.to_datetime(df["Jahr"].astype(str))
        if station_info:
            df = (
                df.set_index("station_id")
                .join(self.stations)
                .reset_index()
                .rename({"index": "station_id"}, axis=1)
            )

        return df.set_index(["station_id", "Jahr"])

    def _request(self, url: str, cache_filename: Union[str, Path]) -> str:
        cache_filename = self.CACHE_DIR / cache_filename
        if cache_filename.exists():
            return cache_filename.read_text()

        if self.verbose:
            print(f"REQUESTING {url}")
        response = requests.get(url, timeout=3)
        assert response.status_code == 200, f"Got {response.status_code}"

        text = response.text
        os.makedirs(cache_filename.parent, exist_ok=True)
        cache_filename.write_text(text)

        return text

    def _request_dataframe(self, text: str, separator: str = ";", skipinitialspace: bool = True):
        fp = StringIO(text)
        df = pd.read_csv(fp, sep=separator, skipinitialspace=skipinitialspace)
        df.columns = [c.strip() for c in df.columns]
        return df

    def _load_stations(self) -> pd.DataFrame:
        df = (
            self._request_dataframe(self._request(self.STATIONS_URL, "stations_list_CLIMAT_data.txt"))
            .rename({"WMO-Station ID": "station_id"}, axis=1)
            .set_index("station_id")
        )
        for key in ("Latitude", "Longitude", "Height"):
            df.loc[:, key] = df.loc[:, key].astype(float, errors="ignore")
        df.loc[:, "Country"] = df.loc[:, "Country"].map(lambda c: c.strip() if isinstance(c, str) else c)
        return df

    def _iter_ftp_dataframes(
            self,
            directory: str,
            pattern: str = "*",
            cache_directory: Optional[str] = None,
            filenames_reduction: Optional[Callable[[List[str]], List[str]]] = None,
            separator: str = ";",
            skipinitialspace: bool = True
    ) -> Generator[Tuple[str, pd.DataFrame], None, None]:
        for fn, data in self._iter_ftp_files(
            directory=directory,
            pattern=pattern,
            cache_directory=cache_directory,
            filenames_reduction=filenames_reduction,
        ):
            if data:
                yield fn, pd.read_csv(BytesIO(data), delimiter=separator, skipinitialspace=skipinitialspace)

    def _iter_ftp_files(
            self,
            directory: str,
            pattern: str = "*",
            cache_directory: Optional[str] = None,
            filenames_reduction: Optional[Callable[[List[str]], List[str]]] = None,
    ) -> Generator[Tuple[str, bytes], None, None]:

        directory = directory.rstrip("/")
        cache_directory = self.CACHE_DIR / (cache_directory or directory)

        if self.force_ftp or not cache_directory.exists():
            ftp = ftplib.FTP("opendata.dwd.de")
            ftp.login()
            ftp.cwd(directory)
            filenames = []
            ftp.retrlines('NLST', lambda l: filenames.append(l))
            os.makedirs(cache_directory, exist_ok=True)
            if filenames_reduction is not None:
                filenames = filenames_reduction(filenames)
            for fn in tqdm(filenames, desc="caching files"):
                if fnmatch.fnmatch(fn, pattern):
                    if not (cache_directory / fn).exists():
                        with open(cache_directory / fn, "wb") as fp:
                            ftp.retrbinary(f"RETR {fn}", fp.write)

                    with open(cache_directory / fn, "rb") as fp:
                        yield fn, fp.read()

        else:
            filenames = [fn.name for fn in cache_directory.glob(pattern)]
            if filenames_reduction is not None:
                filenames = filenames_reduction(filenames)

            for fn in filenames:
                yield fn, (cache_directory / fn).read_bytes()

    def _iter_ftp_files_http(
            self,
            directory: str,
            pattern: str = "*",
            cache_directory: Optional[str] = None,
    ) -> Generator[Tuple[str, bytes], None, None]:

        directory = directory.rstrip("/")
        cache_directory = self.CACHE_DIR / (cache_directory or directory)

        ftp = ftplib.FTP("opendata.dwd.de")
        ftp.login()
        ftp.cwd(directory)
        filenames = []
        ftp.retrlines('NLST', lambda l: filenames.append(l))
        os.makedirs(cache_directory, exist_ok=True)
        for fn in tqdm(filenames, desc="caching files"):
            if fnmatch.fnmatch(fn, pattern):
                if not (cache_directory / fn).exists():
                    url = f"https://opendata.dwd.de/{directory}/{fn}"
                    self._request(url, cache_filename=(cache_directory / fn).relative_to(self.CACHE_DIR))

        for fn in cache_directory.glob(pattern):
            yield fn.name, fn.read_bytes()


def download_all():
    dwd = DwdGlobalCLIMAT(force_ftp=True, verbose=True)
    for attr in dir(dwd.MonthlyType):
        if not attr.startswith("_"):
            print(f"{attr}:")
            dwd.monthly_dataframe(attr)


if __name__ == "__main__":
    download_all()
    #dwd = DwdGlobalCLIMAT()
    #print(dwd.stations)
    #df = dwd.monthly_air_temperature(station_info=True)
    #print(df)