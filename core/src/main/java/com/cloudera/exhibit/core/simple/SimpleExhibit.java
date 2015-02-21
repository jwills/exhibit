package com.cloudera.exhibit.core.simple;

import com.cloudera.exhibit.core.Exhibit;
import com.cloudera.exhibit.core.ExhibitDescriptor;
import com.cloudera.exhibit.core.Frame;
import com.cloudera.exhibit.core.Obs;
import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.base.Function;
import com.google.common.collect.Maps;

import java.util.Map;

public class SimpleExhibit implements Exhibit {

  private final Obs attributes;
  private final Map<String, Frame> frames;

  public static SimpleExhibit of(String name, Frame frame, Object... args) {
    Map<String, Frame> m = Maps.newHashMap();
    m.put(name, frame);
    for (int i = 0; i < args.length; i += 2) {
      m.put(args[i].toString(), (Frame) args[i + 1]);
    }
    return new SimpleExhibit(Obs.EMPTY, m);
  }

  public SimpleExhibit(Obs attributes, Map<String, Frame> frames) {
    this.attributes = attributes;
    this.frames = frames;
  }

  @Override
  public ExhibitDescriptor descriptor() {
    return new ExhibitDescriptor(attributes.descriptor(), Maps.transformValues(frames, new Function<Frame, ObsDescriptor>() {
      @Override
      public ObsDescriptor apply(Frame frame) {
        return frame.descriptor();
      }
    }));
  }

  @Override
  public Obs attributes() {
    return attributes;
  }

  @Override
  public Map<String, Frame> frames() {
    return frames;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Obs:\n");
    sb.append(attributes);
    sb.append("\n");
    for (Map.Entry<String, Frame> e : frames.entrySet()) {
      sb.append("Frame ").append(e.getKey()).append(": [\n");
      for (Obs obs : e.getValue()) {
        sb.append(obs).append(",\n");
      }
      sb.append("]");
    }
    return sb.toString();
  }
}
