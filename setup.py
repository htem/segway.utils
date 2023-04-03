from setuptools import setup

setup(
        name='segway.utils',
        version='1.0',
        url='https://github.com/htem/segway.utils',
        author='Tri Nguyen',
        author_email='tri_nguyen@hms.harvard.edu',
        license='MIT',
        packages=[
            'segway.utils',
            'segway.utils.mongodb_to_sqlite',
            'segway.utils.merge_meshes',
            'segway.utils.convert_zarr_to_hierarchical',
            'segway.utils.post_gist',
        ],
        install_requires=[
            "bson",
            "pymongo",
        ]
)
