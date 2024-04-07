from loguru import logger


def test_config_through_creation():
    from config import Config, conf

    new_conf = Config(value=2)
    logger.info(f"conf: {conf} {conf.value}")
    logger.info(f"new_conf: {new_conf} {new_conf.value}")

    assert new_conf.value == conf.value and new_conf is conf
