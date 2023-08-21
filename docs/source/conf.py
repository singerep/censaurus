# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import sys
import os
sys.path.insert(0, os.path.abspath('../..'))

project = 'censaurus'
copyright = '2023, Ethan Singer'
author = 'Ethan Singer'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx.ext.autodoc', 'sphinx.ext.coverage', 'sphinx.ext.napoleon', 'sphinx.ext.autosectionlabel', 'sphinx.ext.linkcode']
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

html_css_files = [
    'css/custom.css',
]

def linkcode_resolve(domain, info):
    if domain != 'py':
        return None

    module = info['module'].replace('.', '/')    
    return f"https://github.com/singerep/censaurus/blob/main/{module}.py"