import sys
from setuptools import setup, find_packages


version = '0.2.1'

install_requires = [
    'SQLAlchemy>=0.8'
    ]


setup_requires = [
    'nose'
    ]

tests_require = install_requires + [
    'psycopg2'
    ]


if sys.version_info[0] == 2:
    # Shapely is only compatible with Python 2.x
    tests_require.append('shapely')


setup(name='GeoAlchemy2',
      version=version,
      description="Using SQLAlchemy with Spatial Databases",
      long_description=open('README.rst').read(),
      classifiers=[
          "Development Status :: 3 - Alpha",
          "Environment :: Plugins",
          "Operating System :: OS Independent",
          "Programming Language :: Python",
          "Intended Audience :: Information Technology",
          "License :: OSI Approved :: MIT License",
          "Topic :: Scientific/Engineering :: GIS"
      ],
      keywords='geo gis sqlalchemy orm',
      author='Eric Lemoine',
      author_email='eric.lemoine@gmail.com',
      url='http://geoalchemy.org/',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests', "doc"]),
      include_package_data=True,
      zip_safe=False,
      install_requires=install_requires,
      setup_requires=setup_requires,
      tests_require=tests_require,
      test_suite="geoalchemy2.tests",
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
