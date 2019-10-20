import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pretty-eikon",
    version="0.0.1",
    author="Maciej Wlazlo & Pawel Pomorski",
    author_email="pawel.p.pomorski@gmail.com",
    description="Package for bulk imports of news and timeseries from Refinitiv Eikon",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pwpmp/pretty-eikon",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
