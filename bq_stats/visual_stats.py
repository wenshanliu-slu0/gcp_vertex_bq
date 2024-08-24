"""BQ Visual Stats."""

import base64
import io
import urllib.parse

from google.cloud.bigquery import Client  # pylint: disable=no-name-in-module
from IPython.core import magic_arguments
from IPython.display import display
from IPython.display import IFrame
from jinja2 import Environment
from jinja2 import PackageLoader
from jinja2 import select_autoescape
import matplotlib.pyplot as plt
from tqdm import tqdm

TOP_NUMBER_OF_VALUES = 20
MAX_LABEL_LENGTH = 48
SUPPORTED_TYPES = [
    'BOOL', 'DATE', 'DATETIME', 'INT64', 'NUMERIC', 'DECIMAL', 'BIGNUMERIC',
    'BIGDECIMAL', 'FLOAT64', 'STRING', 'TIME', 'TIMESTAMP'
]
NUMERIC_STATS_TYPES = [
    'INT64', 'NUMERIC', 'DECIMAL', 'BIGNUMERIC', 'BIGDECIMAL', 'FLOAT64'
]


def column_statistics(client, table, column, numeric_stats):
  """Retrieve column statistics for a column in a table."""
  job = column_statistics_query_job(client, table, column, numeric_stats)
  return job.to_dataframe()


def column_statistics_query_job(client, table, column, numeric_stats):
  """Retrieve query job for column statistics."""
  build_query = """
      SELECT
      "{column}" as name,
      COUNT(1) as observations,
      COUNT(DISTINCT(`{column}`)) as Distinct_Count,
      100.0 * COUNT(DISTINCT(`{column}`)) / COUNT(1) as Distinct_Count_Pct,
      COUNTIF(`{column}` IS NULL) as Is_Missing_Count,
      100.0 * COUNTIF(`{column}` IS NULL) / COUNT(1) as Is_Missing_Count_Pct,
"""
  if numeric_stats:
    build_query += """
      COUNTIF(IS_INF(`{column}`)) as Is_Infinite_Count,
      100.0 * COUNTIF(IS_INF(`{column}`)) / COUNT(1) as Is_Infinite_Count_Pct,
      AVG(`{column}`) as Average,
      Min(`{column}`) as Minimum,
      Max(`{column}`) as Maximum,
      COUNTIF(`{column}` = 0) as Zero_Count,
      100.0 * COUNTIF(`{column}` = 0) / COUNT(1) as Zero_Count_Pct,
      COUNTIF(`{column}` < 0) as Negative_Count,
      100.0 * COUNTIF(`{column}` < 0) / COUNT(1) as Negative_Count_Pct,
"""
  build_query += """
      FROM `{table}`
"""

  query = build_query.format(table=table, column=column)
  return client.query(query)


def column_counts_chart(client, table, column, top, ax):
  """Output column count for a column in a table."""
  job = column_counts_query_job(client, table, column, top)
  df = job.to_dataframe()
  column_counts_chart_create(df, column, top, ax)


def column_counts_chart_create(df, column, top, ax):
  """Output column count for a dataframe of counts."""
  ax.barh([str(x) for x in df[column]], [int(x) for x in df['count']])
  ax.set_yticks([str(x) for x in df[column]])
  ax.set_yticklabels([str(x) for x in df[column]])
  ax.invert_yaxis()
  ax.set_title('%s - Counts (Top %s)' % (column, top))

  labels = []
  for tick_label in ax.yaxis.get_ticklabels():
    label = tick_label.get_text()
    if len(label) > MAX_LABEL_LENGTH:
      labels.append('%s...' % label[0:MAX_LABEL_LENGTH])
    else:
      labels.append(label)
  ax.yaxis.set_ticklabels(labels)


def column_counts_query_job(client, table, column, top):
  """Retrieve column counts query job."""

  query = """
  SELECT
      `{column}`,
      Count(`{column}`) as count
      FROM `{table}`
      GROUP BY `{column}`
      ORDER BY count DESC
      LIMIT {top}
  """.format(table=table, column=column, top=top)

  return client.query(query)


def bigquery_table_statistics_schema(client, project, dataset, table):
  """Retrieve bigquery table schema dataframe."""
  query = """
    SELECT
      *
    FROM
      `{project}`.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    WHERE
      table_name="{table}"
  """.format(
      project=project,
      dataset=dataset,
      table=table,
  )
  job = client.query(query)
  df_schema = job.to_dataframe()

  return df_schema


def figure_base64(fig):
  """Convert matplotlib figure to a base64 png."""
  img = io.BytesIO()
  fig.savefig(img, format='png', bbox_inches='tight')
  img.seek(0)

  return base64.b64encode(img.getvalue())


def column_counts_chart_png(client, table, column, top):
  """Create HTML base64 data src png of column counts."""
  fig, axs = plt.subplots(1, 1)

  column_counts_chart(
      client=client,
      table=table,
      column=column,
      top=top,
      ax=axs,
  )

  encoded = figure_base64(fig)
  plt.close()

  return 'data:image/png;base64,{}'.format(encoded.decode('utf-8'))


def get_schema(client, project, dataset, table):
  with tqdm(total=100, desc='Getting table schema...') as pbar:
    df_schema = bigquery_table_statistics_schema(client, project, dataset,
                                                 table)
    pbar.update(100)
  return df_schema


def get_column_stats_jobs(client, df_schema):
  """Retrieve query jobs for each column in a table schema."""
  jobs = []

  with tqdm(df_schema.iterrows(), desc='Starting query jobs...') as t:
    for _, row in t:
      t.set_description("Querying data in column '%s'" % row['field_path'])
      if (row['data_type'] in SUPPORTED_TYPES and row['field_path']
          == row['column_name']):  # Nested structures are not supported

        table_id = '%s.%s.%s' % (row['table_catalog'], row['table_schema'],
                                 row['table_name'])
        jobs.append({
            'supported':
                True,
            'job':
                column_statistics_query_job(
                    client=client,
                    table=table_id,
                    column=row['field_path'],
                    numeric_stats=(row['data_type'] in NUMERIC_STATS_TYPES)),
            'row':
                row,
        })

      else:
        jobs.append({
            'supported': False,
            'job': None,
            'row': row,
        })
  return jobs


def get_table_columns_stats_data(client, table_id, jobs):
  """Retrieve template variables data for each table column job."""
  variables = []

  with tqdm(jobs, desc='Retrieve stats...') as t:
    for job in t:

      t.set_description("Retrieve stats for '%s'" % job['row']['field_path'])

      if not job['supported']:
        variables.append({
            'type': 'not_supportted',
            'name': job['row']['field_path'],
        })
      else:
        df_stats = job['job'].to_dataframe()

        if job['row']['data_type'] in NUMERIC_STATS_TYPES:
          variables.append({
              'type':
                  'numeric',
              'name':
                  job['row']['field_path'],
              'observations':
                  df_stats['observations'][0],
              'Distinct_Count':
                  df_stats['Distinct_Count'][0],
              'Distinct_Count_Pct':
                  '%.1f' % df_stats['Distinct_Count_Pct'][0],
              'Is_Missing_Count':
                  df_stats['Is_Missing_Count'][0],
              'Is_Missing_Count_Pct':
                  ('%.1f' % df_stats['Is_Missing_Count_Pct'][0]),
              'Is_Infinite_Count':
                  df_stats['Is_Infinite_Count'][0],
              'Is_Infinite_Count_Pct':
                  '%.1f' % (df_stats['Is_Infinite_Count_Pct'][0]),
              'Zero_Count':
                  df_stats['Zero_Count'][0],
              'Zero_Count_Pct':
                  '%.1f' % df_stats['Zero_Count_Pct'][0],
              'Average':
                  '%.1f' % df_stats['Average'][0],
              'Minimum':
                  '%.1f' % df_stats['Minimum'][0],
              'Maximum':
                  '%.1f' % df_stats['Maximum'][0],
              'Negative_Count':
                  df_stats['Negative_Count'][0],
              'Negative_Count_Pct':
                  '%.1f' % df_stats['Negative_Count_Pct'][0],
              'counts_img':
                  column_counts_chart_png(
                      client=client,
                      table=table_id,
                      column=job['row']['field_path'],
                      top=TOP_NUMBER_OF_VALUES,
                  ),
          })
        else:
          variables.append({
              'type':
                  'categorical',
              'name':
                  job['row']['field_path'],
              'observations':
                  df_stats['observations'][0],
              'Distinct_Count':
                  df_stats['Distinct_Count'][0],
              'Distinct_Count_Pct':
                  '%.1f' % df_stats['Distinct_Count_Pct'][0],
              'Is_Missing_Count':
                  df_stats['Is_Missing_Count'][0],
              'Is_Missing_Count_Pct':
                  ('%.1f' % df_stats['Is_Missing_Count_Pct'][0]),
              'counts_img':
                  column_counts_chart_png(
                      client=client,
                      table=table_id,
                      column=job['row']['field_path'],
                      top=TOP_NUMBER_OF_VALUES,
                  ),
          })

  return variables


@magic_arguments.magic_arguments()
@magic_arguments.argument('table_id', help='An integer positional argument.')
def cell_magic(line, table_id=None):
  """Function that implements the %bigquery_stats magic command."""
  args = magic_arguments.parse_argstring(cell_magic, line)

  table_id = args.table_id

  client = Client()

  table_obj = client.get_table(table_id)

  project, dataset, table = table_id.split('.')

  df_schema = get_schema(client, project, dataset, table)

  jobs = get_column_stats_jobs(client, df_schema)

  variables = get_table_columns_stats_data(client, table_id, jobs)

  # Jinja2 template loader environment
  env = Environment(loader=PackageLoader('bq_stats'),
                    autoescape=select_autoescape())
  template = env.get_template('profile.html')

  height = '1200px'
  width = '100%'

  display(
      IFrame(src='data:text/html;charset=utf-8,' + urllib.parse.quote(
          template.render(
              table_id=table_id,
              number_of_variables=len(variables),
              number_of_observations=table_obj.num_rows,
              number_of_bytes=table_obj.num_bytes,
              variables=variables,
          )),
             height=height,
             width=width))
