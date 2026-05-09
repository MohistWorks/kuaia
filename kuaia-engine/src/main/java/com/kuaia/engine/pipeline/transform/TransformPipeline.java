package com.kuaia.engine.pipeline.transform;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TransformPipeline {
    private final List<PipelineTransform> transforms;
    private final KuaiaRowType outputType;

    private TransformPipeline(KuaiaRowType inputType, List<PipelineConfig.TransformConfig> configs)
            throws PipelineExecutionException {
        List<PipelineTransform> builtTransforms = new ArrayList<>();
        KuaiaRowType currentType = inputType;
        for (PipelineConfig.TransformConfig config : configs) {
            PipelineTransform transform = createTransform(config);
            currentType = transform.outputType(currentType);
            builtTransforms.add(transform);
        }
        this.transforms = Collections.unmodifiableList(builtTransforms);
        this.outputType = currentType;
    }

    public static TransformPipeline from(KuaiaRowType inputType, List<PipelineConfig.TransformConfig> configs)
            throws PipelineExecutionException {
        return new TransformPipeline(inputType, configs);
    }

    public KuaiaRowType getOutputType() {
        return outputType;
    }

    public BinaryRow apply(BinaryRow input) throws PipelineExecutionException {
        BinaryRow current = input;
        for (PipelineTransform transform : transforms) {
            current = transform.apply(current);
        }
        return current;
    }

    private PipelineTransform createTransform(PipelineConfig.TransformConfig config) throws PipelineExecutionException {
        if ("select".equals(config.getType())) {
            return new SelectTransform(config.getFields());
        }
        if ("rename".equals(config.getType())) {
            return new RenameTransform(config.getFrom(), config.getTo());
        }
        throw new PipelineExecutionException("Unsupported transform.type: " + config.getType());
    }
}
