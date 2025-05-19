import json
# Configuration file for the Sphinx documentation builder.

# -- Project information

project = 'Resampling datasets'
copyright = '2024, Willem Boone'
author = 'Willem Boone'


def extract_version_from_file(file_path):
    try:
        # Open and read the JSON file
        with open(file_path, 'r') as file:
            data = json.load(file)

        # Extract and return the version
        return data.get('version', 'Version not found')

    except FileNotFoundError:
        return f"Error: The file '{file_path}' was not found."

    except json.JSONDecodeError:
        return "Error: The file could not be decoded as JSON."

    except Exception as e:
        return f"An unexpected error occurred: {e}"


# release = '0.1'
# version = '0.1.0'

codemeta = "../../codemeta.json"
version = extract_version_from_file(codemeta)
# print(version)


# -- General configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon'
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']

templates_path = ['_templates']

# -- Options for HTML output
html_theme = 'sphinx_rtd_theme'

# -- Options for EPUB output
epub_show_urls = 'footnote'

html_context = {
    "display_github": True, # Integrate GitHub
    "github_user": "willem0boone", # Username
    "github_repo": "Edito_resampling_datasets", # Repo name
    "github_version": "master", # Version
    "conf_py_path": "/docs/source/", # Path in the checkout to the docs root
}