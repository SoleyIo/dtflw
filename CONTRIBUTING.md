# Contributing to dtflw
__It is so great to have you here!__ Thank you.  
We want to make contributing to this project as easy and transparent as possible. 

Here are the contribution which we expect:

1. Reporting a bug
2. Discussing the current state of the code
3. Submitting a fix
4. Proposing new features

## We develop with GitHub
We use github to host code, to track issues and feature requests, as well as accept pull requests.

## We Use GitHub flow

Thus, all changes happen through pull requests.

Pull requests are the best way to propose changes to the codebase. We actively welcome your pull requests. 

So, please:
1. Fork the repo 
2. and follow the process defined by [GitHub flow](https://docs.github.com/en/get-started/quickstart/github-flow).

## Reporting a bug & suggesting a feature

Please, use _Issues_ page of the project. We have prepared templates for this.

## Use a consistent coding style
- We follow (almost always) [this Python style guidelines](https://peps.python.org/pep-0008/#introduction). Thus, we also kindly ask you to do so.
- 4 spaces for indentation.

## Pull request checklist
After you have added changes, please, do not forget to
- [ ] Add [unit tests](#unit-tests) if applicable.
- [ ] [Log the most prominent changes](#log-changes).
- [ ] [Update the version](#versioning) in [setup.py](setup.py).

Now, it should be ready for a review.
## Unit tests

- We love unit tests. Please, cover a newly added code with tests.
- We use [unittest](https://docs.python.org/3/library/unittest.html) framework.
- Tests are also a good source for learning `dtflw` API and how it works.

## Log changes
We document all notable changes in [CHANGE.md](CHANGES.md) file. Please, use the template below to add a new entry __at the top of the document__:
```
## [{new version}] - {date when pushed}
- { Added this feature|class|function }.
- { Refactored this module }.
```

## Versioning

We use [semantic versioning](https://semver.org/#summary): `MAJOR.MINOR.PATCH` where we update

1. `MAJOR` version when you make incompatible API changes
2. `MINOR` version when you add functionality in a backwards compatible manner
3. `PATCH` version when you make backwards compatible bug fixes

## License
When you submit code changes, your submissions are understood to be under the same [BSD 3-Clause License](LICENSE) that covers the project. Feel free to contact the maintainers if that's a concern.