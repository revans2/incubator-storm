package backtype.storm;

import java.util.Map;

/**
 * Provides functionality for validating configuration fields.
 */
public class ConfigValidation {

    /**
     * Declares methods for validating configuration values.
     */
    public static interface FieldValidator {
        /**
         * Validates the given field.
         * @param name the name of the field.
         * @param field The field to be validated.
         * @throws IllegalArgumentException if the field fails validation.
         */
        public void validateField(String name, Object field) throws IllegalArgumentException;
    }
    
    /**
     * Declares a method for validating configuration values that is nestable.
     */
    public static abstract class NestableFieldValidator implements FieldValidator {
        @Override
        public void validateField(String name, Object field) throws IllegalArgumentException {
            validateField(null, name, field);
        }
        
        /**
         * Validates the given field.
         * @param pd describes the parent wrapping this validator.
         * @param name the name of the field.
         * @param field The field to be validated.
         * @throws IllegalArgumentException if the field fails validation.
         */
        public abstract void validateField(String pd, String name, Object field) throws IllegalArgumentException;
    }

    /**
     * Returns a new FieldValidator for a given class.
     * @param cls the Class the field should be a type of
     * @return a FieldValidator for that class
     */
    public static NestableFieldValidator fv(final Class cls) {
        return new NestableFieldValidator() {
            @Override
            public void validateField(String pd, String name, Object field)
                    throws IllegalArgumentException {
                if (field == null) {
                    // A null value is acceptable.
                    return;
                }
                if (! cls.isInstance(field)) {
                    throw new IllegalArgumentException(
                        pd + name + " must be a " + cls.getName() + ". ("+field+")");
                }
            }
        };
    }
    
    /**
     * Returns a new FieldValidator for a List of the given Class.
     * @param cls the Class of elements composing the list
     * @return a FieldValidator for a list of the given class
     */
    public static NestableFieldValidator listFv(Class cls) {
      return listFv(fv(cls));
    }
    
    public static NestableFieldValidator listFv(final NestableFieldValidator validator) {
        return new NestableFieldValidator() {
            @Override
            public void validateField(String pd, String name, Object field)
                    throws IllegalArgumentException {
                if (field == null) {
                    // A null value is acceptable.
                    return;
                }
                if (field instanceof Iterable) {
                    for (Object e : (Iterable)field) {
                        validator.validateField(pd + "Each element of the list ", name, e);
                    }
                    return;
                }
                throw new IllegalArgumentException(
                        "Field " + name + " must be an Iterable" + field.getClass());
            }
        };
    }

    /**
     * Returns a new FieldValidator for a Map of key to val.
     * @param key the Class of keys in the map
     * @param val the Class of values in the map
     * @return a FieldValidator for a Map of key to val
     */
    public static NestableFieldValidator mapFv(final Class key, final Class val) {
        return mapFv(fv(key), fv(val));
    }
    
    public static NestableFieldValidator mapFv(final NestableFieldValidator key, final NestableFieldValidator val) {
        return new NestableFieldValidator() {
            @SuppressWarnings("unchecked")
            @Override
            public void validateField(String pd, String name, Object field)
                    throws IllegalArgumentException {
                if (field == null) {
                    // A null value is acceptable.
                    return;
                }
                if (field instanceof Map) {
                    for (Map.Entry<Object, Object> entry: ((Map<Object, Object>)field).entrySet()) {
                      key.validateField("Each key of the map ", name, entry.getKey());
                      val.validateField("Each value in the map ", name, entry.getValue());
                    }
                    return;
                }
                throw new IllegalArgumentException(
                        "Field " + name + " must be a Map");
            }
        };
    }
    
    /**
     * Validates a list of Numbers.
     */
    public static Object NumbersValidator = listFv(Number.class);

    /**
     * Validates a list of Strings.
     */
    public static Object StringsValidator = listFv(String.class);
    
    /**
     * Validates a map of Strings to Numbers.
     */
    public static Object MapOfStringToNumberValidator = mapFv(String.class, Number.class);

    /**
     * Validates a power of 2.
     */
    public static Object PowerOf2Validator = new FieldValidator() {
        @Override
        public void validateField(String name, Object o) throws IllegalArgumentException {
            if (o == null) {
                // A null value is acceptable.
                return;
            }
            final long i;
            if (o instanceof Number &&
                    (i = ((Number)o).longValue()) == ((Number)o).doubleValue())
            {
                // Test whether the integer is a power of 2.
                if (i > 0 && (i & (i-1)) == 0) {
                    return;
                }
            }
            throw new IllegalArgumentException("Field " + name + " must be a power of 2.");
        }
    };
}
