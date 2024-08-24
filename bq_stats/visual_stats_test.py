"""Test visual_stats module."""
import unittest
from unittest.mock import ANY  # pylint: disable=g-importing-member
from unittest.mock import Mock  # pylint: disable=g-importing-member
from unittest.mock import patch  # pylint: disable=g-importing-member

from bq_stats import visual_stats
import pandas as pd


class AnyStringWith(str):

  def __eq__(self, other):
    return self in other


class TestBigQueryTableStatsCount(unittest.TestCase):
  """Test BigQuery table statistics and counts."""

  def test_column_counts_query_job(self):
    client = Mock()
    client.query = Mock()
    visual_stats.column_counts_query_job(
        client,
        table='table',
        column='test_column',
        top=20,
    )
    client.query.assert_called_with(AnyStringWith('`test_column`'))

  def test_column_counts_chart_create(self):
    ax = Mock()
    ax.yaxis = Mock()
    short_text = Mock()
    short_text.get_text = Mock(return_value='SHORT')
    long_text = Mock()
    long_text.get_text = Mock(
        return_value='LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_LONG_')
    ax.yaxis.get_ticklabels = Mock(return_value=[short_text, long_text])
    visual_stats.column_counts_chart_create(df=pd.DataFrame(
        data=[['a', 2], ['b', 4]], columns=['column', 'count']),
                                            column='column',
                                            top=20,
                                            ax=ax)

  def test_column_counts_chart(self):
    client = Mock()
    table = 'table'
    column = 'column'
    top = 20
    ax = Mock()
    df = pd.DataFrame()
    job = Mock()
    job.to_dataframe = Mock(return_value=df)
    with patch.object(visual_stats, 'column_counts_query_job',
                      return_value=job):
      with patch.object(visual_stats, 'column_counts_chart_create'):
        visual_stats.column_counts_query_job = Mock(return_value=job)
        visual_stats.column_counts_chart_create = Mock()
        visual_stats.column_counts_chart(client, table, column, top, ax)
        visual_stats.column_counts_query_job.assert_called_with(
            client, table, column, top)
        visual_stats.column_counts_chart_create.assert_called_with(
            df, column, top, ax)

  def test_column_statistics(self):
    client = Mock()
    table = 'table'
    column = 'column'
    numeric_stats = True
    df = Mock()
    job = Mock()
    job.to_dataframe = Mock(return_value=df)
    with patch.object(visual_stats,
                      'column_statistics_query_job',
                      return_value=job):
      self.assertEqual(
          df,
          visual_stats.column_statistics(client, table, column, numeric_stats))
      visual_stats.column_statistics_query_job.assert_called_with(
          client, table, column, numeric_stats)

  def test_column_statistics_query_job(self):
    client = Mock()
    client.query = Mock()
    table = 'table'
    column = 'column'
    visual_stats.column_statistics(client, table, column, False)
    client.query.assert_called_with(AnyStringWith('"column" as name'))
    visual_stats.column_statistics(client, table, column, True)
    client.query.assert_called_with(AnyStringWith('Is_Infinite_Count'))

  def test_column_counts_chart_png(self):
    client = Mock()
    table = 'table'
    column = 'column'
    top = 10

    png = Mock()
    png.decode = Mock(return_value='base64png')
    with patch.object(visual_stats, 'column_counts_chart'):
      with patch.object(visual_stats, 'figure_base64', return_value=png):
        expected = 'data:image/png;base64,base64png'
        got = visual_stats.column_counts_chart_png(client, table, column, top)
        self.assertEqual(expected, got)
        visual_stats.column_counts_chart.assert_called_with(client=client,
                                                            table=table,
                                                            column=column,
                                                            top=top,
                                                            ax=ANY)

  def test_get_column_stats_jobs(self):
    client = Mock()
    columns = [
        'data_type', 'field_path', 'column_name', 'table_catalog',
        'table_schema', 'table_name'
    ]

    df = Mock()
    job = Mock()
    job.to_dataframe = Mock(return_value=df)
    with patch.object(visual_stats,
                      'column_statistics_query_job',
                      return_value=job):

      # Non Numerical Statistic
      visual_stats.get_column_stats_jobs(
          client,
          pd.DataFrame(data=[[
              'STRING', 'test_column', 'test_column', 'test_catalog',
              'test_schema', 'test_table'
          ]],
                       columns=columns))
      visual_stats.column_statistics_query_job.assert_called_with(
          client=client,
          table='test_catalog.test_schema.test_table',
          column='test_column',
          numeric_stats=False,
      )

      # Numerical Statistic
      visual_stats.get_column_stats_jobs(
          client,
          pd.DataFrame(data=[[
              'INT64', 'test_column', 'test_column', 'test_catalog',
              'test_schema', 'test_table'
          ]],
                       columns=columns))
      visual_stats.column_statistics_query_job.assert_called_with(
          client=client,
          table='test_catalog.test_schema.test_table',
          column='test_column',
          numeric_stats=True,
      )

      df_schema = pd.DataFrame(data=[
          [
              'STRING', 'test_column', 'test_column', 'test_catalog',
              'test_schema', 'test_table'
          ],
          [
              'STRING', 'test_column', 'test_schema.test_column',
              'test_catalog', 'test_schema', 'test_table'
          ],
          [
              'ARRAY<STRING>', 'test_column', 'test_column', 'test_catalog',
              'test_schema', 'test_table'
          ],
      ],
                               columns=columns)
      expected = [
          {
              'supported': True,
              'job': job,
              'row': ANY,
          },
          {
              'supported': False,
              'job': None,
              'row': ANY,
          },
          {
              'supported': False,
              'job': None,
              'row': ANY,
          },
      ]
      got = visual_stats.get_column_stats_jobs(client, df_schema)
      self.assertEqual(expected, got)

  def test_get_table_columns_stats_data(self):
    client = Mock()

    df_stats = pd.DataFrame(data=[[
        1000,
        1000,
        100.0,
        0,
        0,
        0,
        0,
        0,
        0,
        10,
        9,
        11,
        0,
        0,
    ]],
                            columns=[
                                'observations',
                                'Distinct_Count',
                                'Distinct_Count_Pct',
                                'Is_Missing_Count',
                                'Is_Missing_Count_Pct',
                                'Is_Infinite_Count',
                                'Is_Infinite_Count_Pct',
                                'Zero_Count',
                                'Zero_Count_Pct',
                                'Average',
                                'Minimum',
                                'Maximum',
                                'Negative_Count',
                                'Negative_Count_Pct',
                            ])
    job = Mock()
    job.to_dataframe = Mock(return_value=df_stats)
    jobs = [
        {
            'supported': True,
            'job': job,
            'row': {
                'field_path': 'table_column1',
                'data_type': 'STRING',
            },
        },
        {
            'supported': True,
            'job': job,
            'row': {
                'field_path': 'table_column2',
                'data_type': 'INT64',
            },
        },
        {
            'supported': False,
            'job': job,
            'row': {
                'field_path': 'table_column3',
                'data_type': 'Array<STRING>',
            },
        },
    ]
    with patch.object(visual_stats,
                      'column_counts_chart_png',
                      return_value='base64'):

      expected = [{
          'type': 'categorical',
          'name': 'table_column1',
          'observations': 1000,
          'Distinct_Count': 1000,
          'Distinct_Count_Pct': '100.0',
          'Is_Missing_Count': 0,
          'Is_Missing_Count_Pct': '0.0',
          'counts_img': 'base64'
      }, {
          'type': 'numeric',
          'name': 'table_column2',
          'observations': 1000,
          'Distinct_Count': 1000,
          'Distinct_Count_Pct': '100.0',
          'Is_Missing_Count': 0,
          'Is_Missing_Count_Pct': '0.0',
          'Is_Infinite_Count': 0,
          'Is_Infinite_Count_Pct': '0.0',
          'Zero_Count': 0,
          'Zero_Count_Pct': '0.0',
          'Average': '10.0',
          'Minimum': '9.0',
          'Maximum': '11.0',
          'Negative_Count': 0,
          'Negative_Count_Pct': '0.0',
          'counts_img': 'base64'
      }, {
          'type': 'not_supportted',
          'name': 'table_column3'
      }]
      got = visual_stats.get_table_columns_stats_data(client, 'table_id', jobs)
      self.assertEqual(expected, got)
