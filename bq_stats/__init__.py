"""Workbench Server Extensions."""

from bq_stats.visual_stats import cell_magic as visual_stats_magic


def load_ipython_extension(ipython):
  """Called by IPython when this module is loaded as an IPython extension."""
  ipython.register_magic_function(visual_stats_magic,
                                  magic_kind='line',
                                  magic_name='bigquery_stats')
