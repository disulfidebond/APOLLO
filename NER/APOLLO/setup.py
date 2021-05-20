from setuptools import setup, find_packages
setup(
    name='APOLLO',
    version='0.1',
    author='Iain McConnell and Meysam Ghaffari',
    author_email='iain.mcconnell@wisc.edu',
    description='Automated Public Outbreak Localization though Lexical Operations',
    url='',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent'
    ],
    python_requires='>=3.8.5',
    install_requires=[
        'tqdm',
        'torch',
        'transformers',
        'fuzzywuzzy'

    ]
)
