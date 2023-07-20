import pandas as pd
import httpx
import json

from scenarios_service_client.client import InputGenerator, ScenarioServiceClient

from app.hobs.hobs_tools import HobsTools
from app.config import get_settings, read_json

dndc_version = 'dev/dndc-d6ebda1'

settings = get_settings()
ss_client = ScenarioServiceClient(timeout=3600)
hobs_tools = HobsTools(
    settings.REGROW_EMAIL,
    read_json(settings.PATH_GOOGLE_CREDENTIALS),    
)

# df_hobs_table = hobs_tools.get_hobs_table()

db_concat_tables = hobs_tools.get_concat_tables('production')

hobs_tools.get_db_table_names(db_concat_tables)

df_sites = pd.read_sql_query("SELECT * FROM sites", db_concat_tables)
df_treatments = pd.read_sql_query("SELECT * FROM treatments", db_concat_tables)
df_study = pd.read_sql_query("SELECT * FROM study", db_concat_tables)
df_scenarios = pd.read_sql_query("SELECT * FROM scenarios", db_concat_tables)
df_soils = pd.read_sql_query("SELECT * FROM soils", db_concat_tables)
df_irrigation_events = pd.read_sql_query(
    "SELECT * FROM irrigation_events", db_concat_tables)
df_biomass = pd.read_sql_query(
    "SELECT * FROM biomass", db_concat_tables)
df_crop_reductions = pd.read_sql_query(
    "SELECT * FROM crop_reductions", db_concat_tables)
df_crop_events = pd.read_sql_query(
    "SELECT * FROM crop_events", db_concat_tables)
df_till_events = pd.read_sql_query(
    "SELECT * FROM till_events", db_concat_tables)


pd.read_sql_query(
    """
    WITH sub as (
        WITH x(study,  site, treatment, crop_variety, yield_meas, firstone, rest) as (
            SELECT 
                study,  site, treatment, crop_variety, yield_meas, 
                substr(planting_id, 1, instr(planting_id, ',')-1) as firstone, substr(planting_id, instr(planting_id, ',')+1) as rest 
            FROM biomass WHERE planting_id like "%,%"
            UNION ALL
            SELECT 
                study,  site, treatment, crop_variety, yield_meas, 
                substr(rest, 1, instr(rest, ',')-1) as firstone, substr(rest, instr(rest, ',')+1) as rest 
            FROM x    
            WHERE rest like "%,%" LIMIT 200
        )
        SELECT DISTINCT study,  site, treatment, crop_variety, yield_meas, firstone as planting_id
        FROM x 
        UNION ALL 
        SELECT study,  site, treatment, crop_variety, yield_meas, rest 
        FROM x 
        WHERE 
            rest not like "%,%"  AND
            yield_meas > 0
    ) 
    SELECT 
        study,  site, treatment, crop_variety, yield_meas, planting_id
    FROM sub
    UNION 
    SELECT 
        study, site, treatment, crop_variety, yield_meas, planting_id
    FROM biomass
    WHERE 
        yield_meas > 0 AND
        planting_id NOT LIKE "%,%"
    """, 
    db_concat_tables
)


countries = ['France', 'Germany', 'United Kingdom']
df_euro_crop_studies = pd.read_sql_query(
    f"""
        SELECT
            country, study, site, scenario_name,
            yield, n2o, soc,
            crop_variety, crop_name, treatment
        FROM study
        JOIN(
            SELECT 
                study, site, [scenario name] as scenario_name,
                practice_event_history_ids as event_history_id
            FROM scenarios 
        )
        USING(study)
        JOIN(
            SELECT 
                study, site, country
            FROM sites
        )
        USING(study, site)
        JOIN(
            WITH x(study,  site, treatment, crop_variety, yield_meas, firstone, rest) as (
                SELECT 
                    study,  site, treatment, crop_variety, yield_meas, 
                    substr(planting_id, 1, instr(planting_id, ',')-1) as firstone, substr(planting_id, instr(planting_id, ',')+1) as rest 
                FROM biomass WHERE planting_id like "%,%"
                UNION ALL
                SELECT 
                    study,  site, treatment, crop_variety, yield_meas, 
                    substr(rest, 1, instr(rest, ',')-1) as firstone, substr(rest, instr(rest, ',')+1) as rest 
                FROM x    
                WHERE rest like "%,%" LIMIT 200
            )
            SELECT DISTINCT study,  site, treatment, crop_variety, yield_meas, firstone as planting_id
            FROM x 
            UNION ALL 
            SELECT study,  site, treatment, crop_variety, yield_meas, rest 
            FROM x 
            WHERE 
                rest not like "%,%"  AND
                yield_meas > 0
            UNION
            SELECT DISTINCT
                study,  site, treatment, crop_variety, yield_meas, planting_id
            FROM biomass
            WHERE 
                yield_meas > 0 AND
                planting_id NOT LIKE "%,%"
        )
        USING(study, site)
        JOIN(
            SELECT DISTINCT
                study, planting_id, crop_name
            FROM crop_events
        )
        USING(study, planting_id)
        WHERE
            country IN ({hobs_tools.list_to_string(countries)}) AND
            (yield == 1)
        ORDER BY country, study, site, scenario_name
    """,
    db_concat_tables
)


(df_euro_crop_studies
    .groupby(['crop_name'], as_index=False)
    .apply(lambda df: pd.Series({
        'treatments': len(df.treatment.unique()),
    }))
)

(df_euro_crop_studies
    .groupby(['country', 'crop_name'], as_index=False)
    .apply(lambda df: pd.Series({
        'treatments': len(df.treatment.unique()),
    }))
)


df_drip_studies = pd.read_sql_query(
    f"""
        SELECT
            country, study, site, scenario_name, irrigation_method
        FROM study
        JOIN(
            SELECT 
                study, site, [scenario name] as scenario_name,
                practice_event_history_ids as event_history_id
            FROM scenarios 
        )
        USING(study)
        JOIN(
            SELECT 
                study, site, country
            FROM sites
        )
        USING(study, site)
        JOIN(
            SELECT DISTINCT
                study, event_history_id,
                method as irrigation_method
            FROM irrigation_events
        )
        USING(study, event_history_id)
        WHERE
            irrigation_method == 'drip'
    """,
    db_concat_tables
)


countries = ['France', 'Germany', 'United Kingdom']
df_euro_studies = pd.read_sql_query(
    f"""
        SELECT
            country, study, site, scenario_name,
            yield, n2o, soc
        FROM study
        JOIN(
            SELECT 
                study, site, [scenario name] as scenario_name,
                practice_event_history_ids as event_history_id
            FROM scenarios 
        )
        USING(study)
        JOIN(
            SELECT 
                study, site, country
            FROM sites
        )
        USING(study, site)
        WHERE
            country IN ({hobs_tools.list_to_string(countries)}) AND
            (yield == 1 OR n2o == 1 OR soc == 1)
        ORDER BY country, study, site, scenario_name
    """,
    db_concat_tables
)


germany_studies = df_euro_studies.query("country == 'Germany'")['study'].unique().tolist()

germany_configs_request = hobs_tools.request_configs(
    germany_studies, wait_for_request=False, refresh_config=False
)
hobs_tools.check_request_status(germany_configs_request['request_id'])
germany_configs = hobs_tools.retrieve_config_data(germany_configs_request['request_id'])

# single study test
dndc_sims = ss_client.simulate_from_configs(
    configs=[config['config'] for config in germany_configs if config['study_name'] == 'sehy_2003'],
    project_name='hobs_germany',
    x_consumer_id=settings.REGROW_EMAIL,
    dndc_version=dndc_version,
)
with open(f"/home/curtis/Downloads/test_results.json", "w") as f:
    json.dump(dndc_sims, f)

r = httpx.post(
    "http://api.us.prod.internal:9089/hobs-service/debug/uncertainty-custom-outputs/",
    params = {
        "requester": settings.REGROW_EMAIL,
        "use_treatment_pairing": True,
        "prediction_interval_probability": 0.9,
        "seed": 1996,
        "interpolation_method": "linear",
    },
    files = {"dndc_results": (
        f"/home/curtis/Downloads/test_results.json", 
        open(f"/home/curtis/Downloads/test_results.json", 'rb'),
    )},
    follow_redirects = True,
    timeout = 3600,
)
print(r)
r.text



