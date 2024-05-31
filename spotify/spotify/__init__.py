from dagster import Definitions, load_assets_from_modules, define_asset_job, AssetSelection, ScheduleDefinition

from . import assets

all_assets = load_assets_from_modules([assets])

spotify_job = define_asset_job("spotify_job", selection=AssetSelection.all())

spotify_schedule = ScheduleDefinition(
    job = spotify_job,
    cron_schedule="0 6 * * *" # daily
)

defs = Definitions(
    assets=all_assets,
    schedules=[spotify_schedule]
)
