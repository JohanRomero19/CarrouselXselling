from loguru import logger
from commons.helper import (
        data_reading,
        normalize_json,
        normalize_pays,
        normalize_events,
        aggregate_pays,
        aggregate_events_wide,
        funneling,
        compute_metrics,
    )

def main():
    # data reading
    pays_df = data_reading("../landing/pays.csv", False)
    logger.info(f'count of pays: {pays_df.count()}')
    prints_df = normalize_json(
        data_reading("../landing/prints.json", True),
        'event_data',
        ','
    )
    logger.info(f'count of prints: {prints_df.count()}')
    taps_df = normalize_json(
        data_reading("../landing/taps.json", True),
        'event_data',
        ','
    )
    logger.info(f'count of taps: {taps_df.count()}')

    # normalizing dfs

    pays_normalized_df = normalize_pays(pays_df)
    prints_normalized_df = normalize_events(prints_df)
    taps_normalized_df = normalize_events(taps_df)

    # delete base dfs

    del pays_df, prints_df, taps_df

    # aggregation

    pays_aggregated = aggregate_pays(pays_normalized_df)
    prints_aggregated = aggregate_events_wide(prints_normalized_df, prefix="prints")
    taps_aggregated = aggregate_events_wide(taps_normalized_df, prefix="taps")

    # delete normalized dfs

    del pays_normalized_df, prints_normalized_df, taps_normalized_df

    # merging dfs

    merged_df = funneling(pays_aggregated, prints_aggregated, taps_aggregated, 'outer')
    logger.info(f'count of merged: {merged_df.count()}')

    # delete aggregated

    del pays_aggregated, prints_aggregated, taps_aggregated

    # compute metrics

    metrics_df = compute_metrics(merged_df)
    logger.info(f'count of metrics: {metrics_df.count()}')

    # delete funnel df

    del merged_df

    metrics_df.to_csv('../outputs/resultado.csv')



if __name__ == '__main__':
    main()