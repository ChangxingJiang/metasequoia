from distutils.core import setup

from setuptools import find_packages

with open("README.md", "r", encoding="UTF-8") as file:
    long_description = file.read()

setup(
    name="metasequoia",
    version="0.1.0",
    description="Metasequoia工具箱",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="changxing",
    author_email="1278729001@qq.com",
    url="https://github.com/ChangxingJiang/metasequoia",
    install_requires=["kafka_python>=2.0.2",
                      "PyMySQL>=1.1.0",
                      "sshtunnel>=0.4.0",
                      "streamlit>=1.32.0",
                      "streamlit_app>=0.0.2"],
    license="MIT License",
    packages=find_packages(),
    platforms=["all"],
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Natural Language :: Chinese (Simplified)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Software Development :: Libraries"
    ]
)
