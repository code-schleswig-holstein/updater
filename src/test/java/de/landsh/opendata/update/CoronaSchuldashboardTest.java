package de.landsh.opendata.update;

import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class CoronaSchuldashboardTest {

    @Test
    public void convertFaelleNachKreis() {
        List<List<String>> result = CoronaSchuldashboard.convertFaelleNachKreis("2022-01-12", readJSON("fälle-nach-kreis.json"));
        assertEquals(30, result.size());
        assertEquals("[2022-01-12, Flensburg, Schülerinnen / Schüler, 24]", result.get(0).toString());
        assertEquals("[2022-01-12, Stormarn, Lehrkräfte, 5]", result.get(29).toString());
    }

    @Test
    public void convertFaelleNachSchulart() {
        List<List<String>> result = CoronaSchuldashboard.convertFaelleNachSchulart("2022-01-12", readJSON("fälle-nach-schulart.json"));
        assertEquals(14, result.size());
        assertEquals("[2022-01-12, Grundschule, Schülerinnen / Schüler, 175]", result.get(0).toString());
        assertEquals("[2022-01-12, Berufsbildende Schule, Schülerinnen / Schüler, 84]", result.get(12).toString());
    }

    @Test
    public void convertSchuleGeschlossen() {
        List<List<String>> result = CoronaSchuldashboard.convertSchuleGeschlossen("2022-01-12", readJSON("schule-geschlossen.json"));
        assertEquals(105, result.size());
        assertEquals("[2022-01-12, FL, Grundschule, 0]", result.get(0).toString());
        assertEquals("[2022-01-12, OD, Berufsbildende, 0]", result.get(104).toString());


    }

    @Test
    public void convertSchuleBeeintraechtigt() {
        List<List<String>> result = CoronaSchuldashboard.convertSchuleBeeintraechtigt("2022-01-12", readJSON("schule-beeintraechtigt.json"));
        assertEquals("[2022-01-12, FL, Grundschule, 0]", result.get(0).toString());
        assertEquals("[2022-01-12, HEI, Berufsbildende, 1]", result.get(34).toString());
        assertEquals("[2022-01-12, OD, Berufsbildende, 0]", result.get(104).toString());
        assertEquals(105, result.size());
    }

    @Test
    public void convertTestungenSchule() {
        List<Integer> result = CoronaSchuldashboard.convertTestungenArt(readJSON("test-schule.json"));
        assertEquals(3, result.size());
        assertEquals(584818, (int) result.get(0));
        assertEquals(561022, (int) result.get(1));
        assertEquals(23796, (int) result.get(2));
    }

    @Test
    public void convertTestungenArt() {
        List<Integer> result = CoronaSchuldashboard.convertTestungenArt(readJSON("test-nach-art.json"));
        assertEquals(3, result.size());
        assertEquals(584818, (int) result.get(0));
        assertEquals(36833, (int) result.get(1));
        assertEquals(6089, (int) result.get(2));
    }

    @Test
    public void convertTestungenPositiv() {
        List<Integer> result = CoronaSchuldashboard.convertTestungenPositiv(readJSON("test-positiv.json"));
        assertEquals(3, result.size());
        assertEquals(1948, (int) result.get(0));
        assertEquals(1848, (int) result.get(1));
        assertEquals(100, (int) result.get(2));
    }

    JSONObject readJSON(String resourceName) {
        return new JSONObject(new JSONTokener(getClass().getResourceAsStream("/corona-schuldashboard/" + resourceName)));
    }
}