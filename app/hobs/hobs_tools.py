from typing import List, Dict, Optional
import httpx
import json
import pandas as pd
import numpy as np
import io
import zipfile
import asyncio
import nest_asyncio
import time
import sqlite3

from hobs_service_client.client import HobsServiceClient, StatusEnum, InterpolationMethodEnum
from dndc_eval.utils import upload_json_to_gcs, start_gcs_client, retrieve_from_gcs

class HobsTools:
    def __init__(
        self, 
        user: str,
        gcs_credentials_json: str,
        hobs_url: str = None, 
        hobs_gcs_bucket: str = 'env_sci_scratch',
        hobs_gcs_base_folder: str = 'hobs/temp_configs',
    ):
        self._hobs_client = HobsServiceClient(user)
        nest_asyncio.apply()
        self._gcs_credentials_json = gcs_credentials_json
        self._hobs_url = self._hobs_client.hobs_url.replace('/api/', '/') if hobs_url == None else hobs_url
        self._hobs_gcs_bucket = hobs_gcs_bucket
        self._hobs_gcs_base_folder = hobs_gcs_base_folder

    def get_hobs_table(self) -> pd.DataFrame:
        hobs_table_filename, hobs_table = asyncio.run(self._hobs_client.download_hobs_table())
        df_hobs_table = pd.read_csv(io.BytesIO(hobs_table))
        return df_hobs_table
    
    @staticmethod
    def _remove_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
        duplicates = df.columns.str.endswith(tuple([f'.{num}' for num in range(10)]))
        df = df.loc[:, ~duplicates]
        df.columns = df.columns.str.lower()
        df = df.loc[:, ~df.columns.duplicated()]
        return df
    
    def get_concat_tables(self, stage: str = 'production') -> sqlite3.Connection:
        r = httpx.get(
            url = f"{self._hobs_url}debug/concat-tables",
            params = {'stage': stage},
            timeout = 3600,
            follow_redirects = True
        )
        r.raise_for_status()
        
        zfile = zipfile.ZipFile(io.BytesIO(r.content))
        conn = sqlite3.connect("hobs_concat_tables.db")
        for zipfilename in zfile.filelist:
            try:
                data_piece = pd.read_csv(io.BytesIO(zfile.read(zipfilename)), low_memory=False).set_flags(allows_duplicate_labels=True)
                data_piece = self._remove_duplicate_columns(data_piece)
                file_name = zipfilename.filename.split('.csv')[0].replace('concat_', '')
                data_piece.to_sql(file_name, conn, if_exists='replace')
            except:
                # pass
                print(f"Error reading {zipfilename.filename}")
        
        return conn
    
    @staticmethod
    def get_db_table_names(conn: sqlite3.Connection) -> List[str]:
        return pd.read_sql_query(
            'SELECT name FROM sqlite_master WHERE type= "table";', 
            conn
        ).name.to_list()
    
    @staticmethod
    def list_to_string(items: List) -> str:
        return ', '.join([f"'{item}'" for item in items])
    
    def check_request_status(self, request_id: str) -> Dict:
        return asyncio.run(self._hobs_client.get_status(request_id))
    
    def wait_for_request(self, request_id: str, wait: int = 60) -> StatusEnum:
        status_check = {'request_status': StatusEnum.RUNNING}
        while status_check['request_status'] == StatusEnum.RUNNING:
            status_check = self.check_request_status(request_id)
            if status_check['request_status'] == StatusEnum.SUCCESS:
                return StatusEnum.SUCCESS
            elif status_check['request_status'] == StatusEnum.ERROR:
                return status_check
            else:
                time.sleep(wait)
                continue
    
    def request_configs(
        self, 
        study_names: List[str], 
        stages: list[str] = [], 
        refresh_config: bool = False, 
        wait_for_request: bool = False,
        timeout: int = 6000,
    ) -> List[Dict]:
        params={
            'study_names': study_names,
            'refresh_config': refresh_config,
        }
        if stages:
            params = params | {'stages': stages}
        
        config_request = asyncio.run(self._hobs_client.get_configs(study_names, stages, refresh_config))
        if wait_for_request:
            self.wait_for_request(config_request['request_id'])
            return self.pull_config_data(config_request['request_id'])
        else:
            return config_request
    
    def pull_config_data(
        self, 
        request_id: str,
        timeout: int = 6000,
    ) -> List[Dict]:
        
        results_url = asyncio.run(self._hobs_client.get_result_url(request_id))
        try:
            r = httpx.get(results_url, timeout=timeout, follow_redirects=True)
            r.raise_for_status()
            configs = self._read_zip_from_response(r, 'study_name', 'configs')
            configs = [
                self.extract_hobs_treatment_info(config['study_name'].split('/')[1]) | {'config': config['configs']}
                for config in configs
            ]
            return configs
        except Exception as e:
            raise(str(e))
    
    def save_config_data_to_gcs(
        self,
        request_id: str,
        timeout: int = 6000,
    ) -> None:
        config_data = self.pull_config_data(request_id, timeout)
        folder_name = f"{self._hobs_gcs_base_folder}/{request_id}"
        for data in config_data:
            upload_json_to_gcs(
                data, 
                f"{folder_name}/{data['config']['name']}.json",
                self._hobs_gcs_bucket,
                self._gcs_credentials_json,
                timeout,
            )
    
    def retrieve_config_data(
        self,
        request_id: str,
        timeout: int = 6000,
    ) -> List[Dict]:
        gcs_client = start_gcs_client(self._gcs_credentials_json)
        blobs = gcs_client.list_blobs(
            self._hobs_gcs_bucket, 
            prefix=f"{self._hobs_gcs_base_folder}/{request_id}/"
        )
        confg_data = [json.loads(blob.download_as_string(timeout=timeout)) for blob in blobs]
        return confg_data
    
    def request_mvm_data(
        self,
        study_names: List[str],
        stages: list[str] = [],
        interpolation_method: Optional[InterpolationMethodEnum] = None,
        wait_for_request: bool = False,
    ):
        mvm_request = asyncio.run(self._hobs_client.get_measured_vs_modeled(
            study_names=study_names,
            stages=stages,
            interpolation_method=interpolation_method,
        ))
        request_id = mvm_request['request_id']
        if wait_for_request:
            self.wait_for_request(request_id)
            return self.retrieve_mvm_data(request_id)
        else:
            return request_id
    
    def retrieve_mvm_data(
        self, 
        request_id: str,
        timeout: int = 6000,
    ) -> List[Dict]:
        results_url = asyncio.run(self._hobs_client.get_result_url(request_id))
        try:
            r = httpx.get(results_url, timeout=timeout, follow_redirects=True)
            r.raise_for_status()
            mvms = self._read_zip_from_response(r, 'study_name', 'mvms')
            mvms = [
                mvm | {'study_name': mvm['study_name'].replace('/measured_vs_modeled', '')} 
                for mvm in mvms    
            ]
            return mvms
        except Exception as e:
            raise(str(e))
    
    @staticmethod
    def extract_hobs_treatment_info(name: str):
        return {
            'study_name': name.split('___')[0],
            'site_name': name.split('___')[1],
            'treatment_name': name.split('___')[2],
            'soil_id': name.split('___')[3],
        }
    
    @staticmethod
    def _read_zip_from_response(
        response: httpx.Response, 
        filename_label: str, 
        data_label: str,
    ) -> List[Dict]:
        # TODO could make this a flexible function to return json or csv data
        data = []
        zfile = zipfile.ZipFile(io.BytesIO(response.content))
        for zipfilename in zfile.filelist:
            data_piece = json.loads(zfile.read(zipfilename))
            data += [{filename_label: zipfilename.filename.split('.json')[0], data_label: data_piece}]
        return data

