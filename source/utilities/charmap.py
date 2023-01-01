#! /usr/bin/env -S python3 -B

# The 95 printable ASCII characters.
# This includes the space character (0x20), but excludes control characters (0x00--0x1f) and the DEL character (0x7f).

ASCII = frozenset(" !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~")

# These are the characters that actually occur in the different directives:

occurring_characters = {
    'N' : ASCII | frozenset("\xa0\xad°´·ºÁÃ×àáäåèéíîóöøúüĀńőŜσωआटभयर्ṭ’•…∈≤≥⌈⌉ﬀﬁﬂ"),
    'C' : ASCII | frozenset("¢£§«°±²´·º»½ÁÇ×ÜßàáäåçèéëíîïñòóôõöøùúüýāăćčęěħıłńőřśşšťžΧβγμπρστωϱавдеилмнопрстучшыьяաבוכלᵣᵤḠ\u200b—‘’“”…′ℕ↑⇒∈∏∑∞∩∫≅≈≠≤≥⊂⊆⊗⌈⌉\u3000八發\uf020ﬁﬂ\ufeff𝒩𝓁"),
    'D' : ASCII | frozenset("\x7f§«°±´¸»ÁÇÉÖ×ÚÜßàáäåçèéêëíîïñóôõöøùúüýăąćČčěłńőŒřŚŞşŠšũūżžǎ́Λλμπϕ\u2002\u2009\u200e‐—’“”…∞∪≡ﬀﬁ"),
    'H' : ASCII | frozenset("\x81£§©«®°±´µ·»ÁÂÃÅÆÉÕÖ×ÚÜßàáâäåçèéêëíîïñòóôõöøúûüýĀāăćČčěğĭıłńņňőœřśşŠšţūŽžΓΔΛΣΨαβγδζθπστφωϕНРСагдезийклнопрстхчыяאבגדוכלקרשתṭ\u200e—’“”…∏∑√∣≡⌊⌋ﬀﬁﬂ"),
    'F' : ASCII | frozenset("°²´·ºÁÇ×ÜàáäçèéêíñóôöøúüćńőřşžΓβλ‐‘’”…∞≍≤≥⌈⌉\u3000\ufeff；"),
    'e' : ASCII | frozenset("¢¨¯°´·×ßáäçèéíôöüīńβλρω\u200b—‘’“”•…∆⊗│："),
    'p' : ASCII | frozenset("Äéóöø‘’"),
    't' : ASCII | frozenset("\x8a®°¹¼×áçèéíñóöúüŠπ…\u2028√≠≤≥\u3000\uf08a\uf0a3\uf0ae\uf0b3\uf0b9"),
    'o' : ASCII | frozenset("\x8d£«¯´·»Áßáäçèéêíîïðòö÷üπ“”…€←∪≠⊤⌊⌿⍳⍴⍸○"),
    'Y' : ASCII | frozenset("ßáéñöøńőΧ’…⊂\u3000"),
    'A' : ASCII | frozenset("ÁÅÆÇÉØÜßàáâäçèéëíñóôöøúüČńņőşš"),
    'E' : ASCII | frozenset("´ÁÉßàáãäçèéíñóôöøüýčěłńőš’"),
}

# These are the characters that are deemed acceptable:
#
# Compared to the actually occurring characters, we have removed the following characters from the acceptable characters:
#
#   - the ligature characters: 'ﬀ', 'ﬁ', 'ﬂ'
#   - fullwidth colon character '：' (0xff1a)
#   - fullwidth colon character '；' (0xff1b)

acceptable_characters = {
    'N' : ASCII | frozenset("\xa0\xad°´·ºÁÃ×àáäåèéíîóöøúüĀńőŜσωआटभयर्ṭ’•…∈≤≥⌈⌉"),
    'C' : ASCII | frozenset("¢£§«°±²´·º»½ÁÇ×ÜßàáäåçèéëíîïñòóôõöøùúüýāăćčęěħıłńőřśşšťžΧβγμπρστωϱавдеилмнопрстучшыьяաבוכלᵣᵤḠ\u200b—‘’“”…′ℕ↑⇒∈∏∑∞∩∫≅≈≠≤≥⊂⊆⊗⌈⌉\u3000八發\uf020ﬁﬂ\ufeff𝒩𝓁"),
    'D' : ASCII | frozenset("\x7f§«°±´¸»ÁÇÉÖ×ÚÜßàáäåçèéêëíîïñóôõöøùúüýăąćČčěłńőŒřŚŞşŠšũūżžǎ́Λλμπϕ\u2002\u2009\u200e‐—’“”…∞∪≡"),
    'H' : ASCII | frozenset("\x81£§©«®°±´µ·»ÁÂÃÅÆÉÕÖ×ÚÜßàáâäåçèéêëíîïñòóôõöøúûüýĀāăćČčěğĭıłńņňőœřśşŠšţūŽžΓΔΛΣΨαβγδζθπστφωϕНРСагдезийклнопрстхчыяאבגדוכלקרשתṭ\u200e—’“”…∏∑√∣≡⌊⌋"),
    'F' : ASCII | frozenset("°²´·ºÁÇ×ÜàáäçèéêíñóôöøúüćńőřşžΓβλ‐‘’”…∞≍≤≥⌈⌉\u3000\ufeff"),
    'e' : ASCII | frozenset("¢¨¯°´·×ßáäçèéíôöüīńβλρω\u200b—‘’“”•…∆⊗│"),
    'p' : ASCII | frozenset("Äéóöø‘’"),
    't' : ASCII | frozenset("\x8a®°¹¼×áçèéíñóöúüŠπ…\u2028√≠≤≥\u3000\uf08a\uf0a3\uf0ae\uf0b3\uf0b9"),
    'o' : ASCII | frozenset("\x8d£«¯´·»Áßáäçèéêíîïðòö÷üπ“”…€←∪≠⊤⌊⌿⍳⍴⍸○"),
    'Y' : ASCII | frozenset("ßáéñöøńőΧ’…⊂\u3000"),
    'A' : ASCII | frozenset("ÁÅÆÇÉØÜßàáâäçèéëíñóôöøúüČńņőşš"),
    'E' : ASCII | frozenset("´ÁÉßàáãäçèéíñóôöøüýčěłńőš’"),
}


def main():

    for key in sorted(acceptable_characters):
        assert occurring_characters[key].issuperset(acceptable_characters[key])

        unwanted_characters = occurring_characters[key] - acceptable_characters[key]

        if len(unwanted_characters) > 0:
            print("key {} has unwanted characters: {}".format(key, ", ".join("{!r}".format(c) for c in sorted(unwanted_characters))))


if __name__ == "__main__":
    main()
