from setuptools import setup
from os.path import join, dirname, abspath


def main():
    reqs_file = join(dirname(abspath(__file__)), 'requirements.txt')
    with open(reqs_file) as f:
        requirements = [req.strip() for req in f.readlines()]

    setup(name='pgsession',
          version='0.0.dev0',
          description='',
          url='http://github.com/baldur/pgsession',
          author='',
          author_email='baldur.gudbjornsson@gmail.com',
          license='MIT',
          packages=['pgsession'],
          install_requires=requirements,
          zip_safe=False)

if __name__ == '__main__':
    main()
