import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="hazpy",
    version="0.0.2",
    author="James Raines,Ujvala K Sharma",
    author_email="james.rainesii@fema.dhs.gov, usharma@niyamit.com, ujvalak_in@yahoo.com",
    description="FEMA - Hazus Open-Source Library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nhrap-dev/hazpy.git",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
