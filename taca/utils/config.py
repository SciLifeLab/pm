""" Load and parse configuration file
"""
import ConfigParser
import os
import yaml

CONFIG = {}

def load_config(config_file=None):
    """Loads a configuration file.

    By default it assumes ~/.taca/taca.yaml
    """
    try:
        if not config_file:
            config_file = os.path.join(os.environ.get('HOME'), '.taca', 'taca.yaml')
        config = ConfigParser.SafeConfigParser()
        with open(config_file) as f:
            config.readfp(f)
        return config
    except IOError:
        raise IOError(("There was a problem loading the configuration file. "
                "Please make sure that ~/.taca/taca.conf exists and that you have "
                "read permissions"))


def load_yaml_config(config_file):
    """Load YAML config file

    :param str config_file: The path to the configuration file.

    :returns: A dict of the parsed config file.
    :rtype: dict
    :raises IOError: If the config file cannot be opened.
    """
    if type(config_file) is file:
        CONFIG.update(yaml.load(config_file) or {})
        return CONFIG
    else:
        try:
            with open(config_file, 'r') as f:
                content = yaml.load(f)
                CONFIG.update(content)
                return content
        except IOError as e:
            e.message = "Could not open configuration file \"{}\".".format(config_file)
            raise e
