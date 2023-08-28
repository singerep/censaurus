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

extensions = ['sphinx.ext.autodoc', 'sphinx.ext.coverage', 'sphinx.ext.napoleon', 'sphinx.ext.autosectionlabel', 'sphinx.ext.linkcode', "sphinxext.opengraph"]
pygments_style = 'sphinx'

templates_path = ['_templates']
exclude_patterns = []

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
    "conf_py_path": "/docs/source/", # Path in the checkout to the docs root,
    "html_theme": 'sphinx_rtd_theme'
}

ogp_site_url = 'https://censaurus.readthedocs.io/en/latest/'
ogp_image = 'https://raw.githubusercontent.com/singerep/censaurus/main/docs/source/censaurus_logo.png'
ogp_image_alt = 'censaurus logo'
ogp_title = 'censaurus: Not just another Census data tool. Seriously.'
ogp_description = 'A comprehensive, feature-rich, and user-oriented Python package that wraps the U.S. Census Bureau\'s Data and Geographic APIs.'
ogp_type = 'object'
ogp_custom_meta_tags = [
    '<meta name="twitter:image:src" content="https://raw.githubusercontent.com/singerep/censaurus/main/docs/source/censaurus_logo.png">',
    '<meta name="twitter:card" content="summary_large_image">',
    '<meta name="twitter:title" content="censaurus: Not just another Census data tool. Seriously.">',
    '<meta name="twitter:description" content="A comprehensive, feature-rich, and user-oriented Python package that wraps the U.S. Census Bureau\'s Data and Geographic APIs.">',
]

html_css_files = [
    'css/custom.css',
]

def linkcode_resolve(domain, info):
    if domain != 'py':
        return None

    module = info['module'].replace('.', '/')    
    return f"https://github.com/singerep/censaurus/blob/main/{module}.py"