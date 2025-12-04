import setuptools

with open('requirements.txt', 'r', encoding='utf-8') as f:
    requirements = [
        line.strip()
        for line in f
        if line.strip() and not line.startswith('#') and not line.startswith('-')
    ]

setuptools.setup(
    name='ecommerce-event-pipeline',
    version='1.0.0',
    description='Real-time e-commerce event processing pipeline using Apache Beam',
    author='Joao Miranda',
    author_email='juanvictor4.c@gmail.com',

    packages=setuptools.find_packages(exclude=['tests', 'tests.*']),
    package_dir={'': '.'},

    include_package_data=True,

    install_requires=requirements,

    python_requires='>=3.9',

    entry_points={
        'console_scripts': [
            'run-pipeline=main:run',
        ],
    }
)
