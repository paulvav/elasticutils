import setuptools

setuptools.setup(
    name="elasticutils",
    version="0.0.2",
    author="Paul Vavich",
    author_email="paulvav@gmail.com",
    description="Utility Functions for Elasticsearch",
    url="https://github.com/paulvav/elasticutils",
    py_modules = ['elasticutils'],
    install_requires = ["requests"]
)