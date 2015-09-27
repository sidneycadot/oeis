
class OeisEntry:
    def __init__(self, oeis_id, identification, values, name, comments, detailed_references, links, formulas, examples,
                 maple_programs, mathematica_programs, other_programs, cross_references, keywords, offset_a, offset_b, author, extensions_and_errors):
        self.oeis_id               = oeis_id
        self.identification        = identification
        self.values                = values
        self.name                  = name
        self.comments              = comments
        self.detailed_references   = detailed_references
        self.links                 = links
        self.formulas              = formulas
        self.examples              = examples
        self.maple_programs        = maple_programs
        self.mathematica_programs  = mathematica_programs
        self.other_programs        = other_programs
        self.cross_references      = cross_references
        self.keywords              = keywords
        self.offset_a              = offset_a
        self.offset_b              = offset_b
        self.author                = author
        self.extensions_and_errors = extensions_and_errors
    def __str__(self):
        return "A{:06d}".format(self.oeis_id)
