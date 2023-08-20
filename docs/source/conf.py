# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'censaurus'
copyright = '2023, Ethan Singer'
author = 'Ethan Singer'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx.ext.autodoc', 'sphinx.ext.coverage', 'sphinx.ext.napoleon', 'sphinx.ext.autosectionlabel']
pygments_style = 'sphinx'

templates_path = ['_templates']
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
autodoc_member_order = 'bysource'
html_static_path = ['_static']

html_logo = 'censaurus_logo.png'
html_favicon = 'censaurus_logo_icon.png'

html_context = {
    "display_github": True, # Integrate GitHub
    'display_version': True,
    "github_user": "singerep", # Username
    "github_repo": "censaurus", # Repo name
    "github_version": "main", # Version
    "conf_py_path": "/docs/source/", # Path in the checkout to the docs root
}

# github_url = "https://github.companyname.com/xyz"