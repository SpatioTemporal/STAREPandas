# Configuration file for the sph documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

sys.path.insert(0, os.path.abspath('../'))

# -- Project information -----------------------------------------------------

project = 'STAREPandas'
copyright = '2021, Niklas Griessbaum'
author = 'Niklas Griessbaum'

# The full version, including alpha/beta/rc tags
release = '2022-09-15'

# -- General configuration ---------------------------------------------------

# Add any sph extension module names here, as strings. They can be
# extensions coming with sph (named 'sphinx.ext.*') or your custom
# ones.
extensions = ['sphinx.ext.autodoc',
              'sphinx.ext.autosummary',
              'sphinx_automodapi.automodapi',
              'myst_parser',  # markdown parsing
              'nbsphinx',  # Notebook integration
              "numpydoc",   # Syntax/schema for docstrings!
              #'m2r2'
              'sphinx_markdown_tables'
              ]

#source_suffix = ['.rst', '.py']

# continue doc build and only print warnings/errors in examples
ipython_warning_is_error = False
ipython_exec_lines = [
    # ensure that dataframes are not truncated in the IPython code blocks
    "import pandas as _pd",
    '_pd.set_option("display.max_columns", 20)',
    '_pd.set_option("display.width", 100)',
]

numpydoc_show_class_members = False

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
add_module_names = False

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
# html_theme = 'alabaster'
# html_theme = 'sphinx_rtd_theme'
html_theme = "pydata_sphinx_theme"
html_theme_options = {
    "collapse_navigation": True,
    "show_toc_level": 1,
    "navbar_align": "content",
    "github_url": "https://github.com/SpatioTemporal/STAREPandas",
}

autosummary_generate = True

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# html_css_files = ['style.css']
html_css_files = ['https://cdn.jupyter.org/notebook/5.1.0/style/style.min.css']


#html_sidebars = {"**": []}

nbsphinx_execute = "auto"
nbsphinx_allow_errors = True
nbsphinx_kernel_name = "python3"
exclude_patterns = ['_build']



nbsphinx_prolog = r"""
{% set docname = env.doc2path(env.docname, base=None) %}

.. only:: html

    .. role:: raw-html(raw)
        :format: html

    .. note::

        | This page was generated from `{{ docname }}`__.
        

        __ https://github.com/SpatioTemporal/STAREPandas/docs/source/{{ docname }}
"""


