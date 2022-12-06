package lt.mk.awskeyspacebackuptos3.statistic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StatisticsRender {


    private final List<RenderData> renders = new ArrayList<>();


    static class RenderData {
        StatProvider provider;
        int[] widths;
        int totalWidth;
    }


    public void add(Statistical statistical) {
        RenderData r = new RenderData();
        r.provider = statistical.provider();
        calcWidths(r);
        renders.add(r);
    }

    private void calcWidths(RenderData r) {
        String[] h2 = r.provider.h2();
        String[] d = r.provider.data();
        fillWidths(r, h2, d);

    }

    private static void fillWidths(RenderData r, String[] h2, String[] d) {
        r.widths = new int[h2.length];
        for (int i = 0; i < h2.length; i++) {
            r.widths[i] = Math.max(h2[i].length(), d[i].length());
        }
        r.totalWidth = Math.max(r.provider.h1().length() + 2, calcWidth(r.widths));
    }

    private static int calcWidth(int[] widths) {
        return Arrays.stream(widths).sum() + 1 + widths.length;
    }


    public String header() {
        StringBuilder b = new StringBuilder();

        b.append(line()).append("\n");
        b.append(headers1()).append("\n");
        b.append(line()).append("\n");
        b.append(headers2()).append("\n");
        b.append(line());

        return b.toString();
    }

    private String line() {
        StringBuilder b = new StringBuilder("|");
        for (RenderData render : renders) {
            if (render.provider.on()) {
                b.append("-".repeat(Math.max(0, render.totalWidth) - 1));
            }
        }
        return b.replace(b.length() - 1, b.length(), "|").toString();
    }

    private String headers1() {
        StringBuilder b = new StringBuilder("|");
        for (RenderData render : renders) {
            if (render.provider.on()) {
                String h1 = render.provider.h1();
                int w = (render.totalWidth - 2);
                int delta = (w - h1.length()) / 2;
                w -= delta;

                b.append(String.format(" ".repeat(delta) + "%-" + w + "s|", h1));
            }
        }
        return b.toString();
    }

    private String headers2() {
        StringBuilder b = new StringBuilder("|");
        for (RenderData render : renders) {
            if (render.provider.on()) {
                String[] h2 = render.provider.h2();
                for (int i = 0; i < render.widths.length; i++) {
                    int w = render.widths[i];
                    int delta = (w - h2[i].length()) / 2;
                    w -= delta;
                    b.append(String.format(" ".repeat(delta) + "%-" + w + "s|", h2[i]));
                }
            }
        }
        return b.toString();

    }

    public String data() {
        StringBuilder b = new StringBuilder("|");
        for (RenderData render : renders) {
            if (render.provider.on()) {
                String[] data = render.provider.data();
                for (int i = 0; i < render.widths.length; i++) {
                    b.append(String.format("%" + (render.widths[i]) + "s|", data[i]));
                }
            }
        }
        return b.toString();
    }
}
