import pandas as pd
def re_part(load_dt = '20150101'):
    path = f'/home/minjoo/data/movie/movie_data/data/extract/load_dt={load_dt}'
    df = pd.read_parquet(path)
    df['load_dt'] = load_dt
    df.to_parquet('~/data/movie/repartition', partition_cols=['load_dt','multiMovieYn', 'repNationCd'])
