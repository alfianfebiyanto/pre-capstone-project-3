import logging

def setup_logger(name: str) -> logging.Logger:

    """
    Mengonfigurasi dan mengembalikan objek logger standar untuk pipeline.

    Args:
        name (str): Nama modul yang menggunakan logger (biasanya __name__).

    Returns:
        logging.Logger: Objek logger yang siap digunakan.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(name)