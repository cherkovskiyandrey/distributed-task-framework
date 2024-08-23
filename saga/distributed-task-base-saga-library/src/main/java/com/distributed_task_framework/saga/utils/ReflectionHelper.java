package com.distributed_task_framework.saga.utils;

import com.distributed_task_framework.task.Task;
import lombok.experimental.UtilityClass;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.Optional;

@UtilityClass
public class ReflectionHelper {

    public <A extends Annotation> Optional<A> findAnnotation(AnnotatedElement annotatedElement, Class<A> annotationCls) {
        A mergedAnnotation = AnnotatedElementUtils.getMergedAnnotation(annotatedElement, annotationCls);
        if (mergedAnnotation != null) {
            return Optional.of(mergedAnnotation);
        }
        @SuppressWarnings("unchecked")
        Class<Task<?>> targetClass = (Class<Task<?>>) AopUtils.getTargetClass(annotatedElement);
        return Optional.ofNullable(AnnotatedElementUtils.getMergedAnnotation(targetClass, annotationCls));
    }
}
