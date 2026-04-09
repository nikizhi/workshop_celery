class PermanentJobError(Exception):
    """
    Ошибка, при которой не нужно запускать задачу повторно.
    Например, некорректный payload или неподдерживаемый job_type.
    """
    pass
