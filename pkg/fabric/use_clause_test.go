package fabric

import "testing"

func TestAdministrationReservedWordsAreASCII(t *testing.T) {
	for _, statement := range []string{"CREATE UſER ordinary", "SHOW UſERS", "ALTER UſER ordinary SET PASSWORD 'synthetic'", "ſHOW DATABASES"} {
		if IsAdministrationCommand(statement) {
			t.Errorf("non-ASCII keyword classified as administration: %q", statement)
		}
	}
	for _, statement := range []string{"cReAtE uSeR `uſK`", "sHoW uSeRs", "aLtEr uSeR `uſK` SET PASSWORD 'synthetic'"} {
		if !IsAdministrationCommand(statement) {
			t.Errorf("ASCII keywords lost administration classification: %q", statement)
		}
	}
}
