#!/usr/bin/env python
from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(
    name="tap-amazon-ads",
    version="0.0.1",
    description="Singer.io tap for extracting data from Amazon Advertising API (version 3).",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="Vibe Inc",
    url="https://github.com/vibeus/tap-amazon-advertising-v3",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_amazon_ads"],
    install_requires=[
        "requests",
        "singer-python",
        "python-dateutil",
        "requests_oauthlib",
        "zlib"
    ],
    entry_points="""
    [console_scripts]
    tap-amazon-ads=tap_amazon_ads:main
    """,
    packages=["tap_amazon_ads", "tap_amazon_ads.streams"],
    package_data={"schemas": ["tap_amazon_ads/schemas/*.json"]},
    include_package_data=True,
)
