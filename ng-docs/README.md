# Patio Documentation

The documentation overhaul started as a HackWeek 2021 project. It was decided to use Docusaurus as the static website generator for documentation, but there's still a lot of work that needs to be done. The Docusaurus README follows this section, and is valid for a development environment. Once docs are fully converted, an effort will need to be made to publish them to Github Pages. You can view the current documentation on [Github Pages](http://c2fo.github.io/patio/index.html).

## How you can help!
There's still a lot of existing documents that need to be converted into Markdown from the original docs Jekyll generated HTML. There's a few ways this can be done, but the way it was done for HackWeek was with a Python Library called [Markdownify](https://github.com/matthewwithanm/python-markdownify). 

### Install Markdownify (MacOS)
```
$ pip3 install markdownify
```

### Quick convert

From `patio` root
```
$ python3
>>> from markdownify import markdownify as md
>>> file = open('./docs/doc.html', 'r')
>>> markdown = md(file)
>>> open('./ng-docs/docs/doc.md', 'w').write(markdown)
```

This will generate a Markdown file in a directory that Docusaurus can pick up. The conversion is not perfect, and some post processing will need to be done to clean some HTML cruft out of the Markdown. It would also be good to compare to the existing documentation to place the doc in the correct sub folder.

Currently, `ng-docs/docs/Classes/patio.md` and `ng-docs/docs/Namespaces/patio.sql.md` have been converted this way to provide an example of the post processing, but there's probably still room for improvement.

---
# Default Docusaurus README.md
---

# Website

This website is built using [Docusaurus 2](https://docusaurus.io/), a modern static website generator.

## Installation

```console
yarn install
```

## Local Development

```console
yarn start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

## Build

```console
yarn build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

## Deployment

```console
GIT_USER=<Your GitHub username> USE_SSH=true yarn deploy
```

If you are using GitHub pages for hosting, this command is a convenient way to build the website and push to the `gh-pages` branch.
