<p align="left">
<img src="./atmostream/logo.png" alt="sidepanel" width="250"  style="display: block; margin-right: auto;">
</p>

# *atmostream* - A Python approach to working with atmospheric forecast model data

This is a library that is used to work with Environment Canada and NOAA/NCEP atmospheric forecast products.

Each one of these products provide atmospheric forecast data at regular intervals everyday (in grib format), with limited to no historical archived data.

This library makes it easy to download, or continuously stream the latest raw forecast data to your local machine. There are also utilities to convert to MIKE DFS file formats on the fly.

Currently, the supported forecast products are:

**Environment Canada**
* HRDPS Continental
* HRDPS North
* RDPS
* GDPS
* GEPS

**NOAA/NCEP**
* NAM CONUS Nest
* CFS

And more are being added.

Right now, only certain variables (winds, pressure, humidity, solar radiation, precipitation) are supported. General support is coming for all forecast variables as the support to convert to MIKE DFS files is developed for all the variable types.

## Installation

To install this library, clone the GitHub repository to your local machine.

Navigate to the main atmostream folder (where the setup.py file resides), and run:

`pip install .`

This library should work on both Windows and Linux, Python versions 3.9+.

## Examples

See the examples.py script in the repository. 

It shows how to set up a streaming service for each of the supported forecast products.

Simply start streaming, and watch the data start to collect!
