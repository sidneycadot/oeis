
class OeisEntry:
    def __init__(self, oeis_id, identification, values, name, offset, keywords):
        self.oeis_id        = oeis_id
        self.identification = identification
        self.values         = values
        self.name           = name
        self.offset         = offset
        self.keywords       = keywords
    def __str__(self):
        return "A{:06d}".format(self.oeis_id)
