from setuptools import setup, find_packages


setup(
    name='GeoAlchemy2',
    use_scm_version=True,
    description="Using SQLAlchemy with Spatial Databases",
    long_description=open('README.rst').read(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Plugins",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: MIT License",
        "Topic :: Scientific/Engineering :: GIS",
    ],
    keywords='geo gis sqlalchemy orm',
    author='Eric Lemoine',
    author_email='eric.lemoine@gmail.com',
    url='http://geoalchemy.org/',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests', 'doc']),
    include_package_data=True,
    zip_safe=False,
    setup_requires=["setuptools_scm"],
    install_requires=[
        'SQLAlchemy>=0.8',
    ],
    entry_points="""
    # -*- Entry points: -*-
    """,
)
