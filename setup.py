from setuptools import find_namespace_packages
from setuptools import setup

setup(
    name="GeoAlchemy2",
    use_scm_version=True,
    description="Using SQLAlchemy with Spatial Databases",
    long_description=open("README.rst", encoding="utf-8").read(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Plugins",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: MIT License",
        "Topic :: Scientific/Engineering :: GIS",
    ],
    keywords="geo,gis,sqlalchemy,orm",
    author="Eric Lemoine",
    author_email="eric.lemoine@gmail.com",
    url="https://geoalchemy-2.readthedocs.io/en/stable/",
    project_urls={
        "Tracker": "https://github.com/geoalchemy/geoalchemy2/issues",
        "Source": "https://github.com/geoalchemy/geoalchemy2",
    },
    license="MIT",
    python_requires=">=3.7",
    packages=find_namespace_packages(include=["geoalchemy2*"]),
    include_package_data=True,
    zip_safe=False,
    setup_requires=["setuptools_scm"],
    install_requires=["SQLAlchemy>=1.4", "packaging"],
    extras_require={
        "shapely": ["Shapely>=1.7"],
    },
    entry_points={
        "sqlalchemy.plugins": [
            "geoalchemy2 = geoalchemy2.admin.plugin:GeoEngine",
        ]
    },
)
