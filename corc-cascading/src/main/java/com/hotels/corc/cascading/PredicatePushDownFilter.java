package com.hotels.corc.cascading;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import com.google.common.base.Objects;

/**
 * A {@link Filter} implementation that also manages a corresponding {@link SearchArgument}. Use this just after a Corc
 * source tap.
 */
public class PredicatePushDownFilter extends BaseOperation<Void> implements Filter<Void> {

  private static final long serialVersionUID = 1L;
  private final CompositePredicate root;

  // TODO Get this into the Corc source tap (some how)
  private final SearchArgument sargs;

  PredicatePushDownFilter(CompositePredicate root, SearchArgument sargs) {
    this.root = root;
    this.sargs = sargs;
  }

  @Override
  public boolean isRemove(@SuppressWarnings("rawtypes") FlowProcess flowProcess, FilterCall<Void> filterCall) {
    return !root.apply(filterCall.getArguments());
  }

  public static class Builder {

    private final Stack<CompositePredicate> predicates;

    public Builder() {
      predicates = new Stack<>();
      predicates.push(new IdentityPredicate());
    }

    public Builder between(Fields fields, Object lower, Object upper) {
      predicates.peek().addPredicate(new BetweenPredicate(fields, lower, upper));
      return this;
    }

    public Builder end() {
      CompositePredicate popped = predicates.pop();
      popped.verify();
      popped.end();
      return this;
    }

    public Builder equals(Fields fields, Object literal) {
      predicates.peek().addPredicate(new EqualsPredicate(fields, literal));
      return this;
    }

    public Builder in(Fields fields, Object... literals) {
      predicates.peek().addPredicate(new InPredicate(fields, literals));
      return this;
    }

    public Builder isNull(Fields fields) {
      predicates.peek().addPredicate(new IsNullPredicate(fields));
      return this;
    }

    public Builder lessThan(Fields fields, Object literal) {
      predicates.peek().addPredicate(new LessThanPredicate(fields, literal));
      return this;
    }

    public Builder lessThanEquals(Fields fields, Object literal) {
      predicates.peek().addPredicate(new LessThanEqualsPredicate(fields, literal));
      return this;
    }

    public Builder greaterThan(Fields fields, Object literal) {
      predicates.peek().addPredicate(new GreaterThanPredicate(fields, literal));
      return this;
    }

    public Builder greaterThanEquals(Fields fields, Object literal) {
      predicates.peek().addPredicate(new GreaterThanEqualsPredicate(fields, literal));
      return this;
    }

    public Builder nullSafeEquals(Fields fields, Object literal) {
      predicates.peek().addPredicate(new NullSafeEqualsPredicate(fields, literal));
      return this;
    }

    public Builder startAnd() {
      AndPredicate andPredicate = new AndPredicate();
      predicates.peek().addPredicate(andPredicate);
      predicates.push(andPredicate);
      return this;
    }

    public Builder startNot() {
      NotPredicate notPredicate = new NotPredicate();
      predicates.peek().addPredicate(notPredicate);
      predicates.push(notPredicate);
      return this;
    }

    public Builder startOr() {
      OrPredicate orPredicate = new OrPredicate();
      predicates.peek().addPredicate(orPredicate);
      predicates.push(orPredicate);
      return this;
    }

    public PredicatePushDownFilter build() {
      if (predicates.size() != 1) {
        throw new IllegalStateException("");
      }
      CompositePredicate root = predicates.peek();
      root.verify();
      SearchArgument sargs = root.getSearchArgument();
      return new PredicatePushDownFilter(root, sargs);
    }
  }

  interface Predicate extends Serializable {
    abstract boolean apply(TupleEntry tupleEntry);

    abstract void verify();

    abstract SearchArgument getSearchArgument();
  }

  static abstract class BasePredicate implements Predicate {
    private static final long serialVersionUID = 1L;

    final SearchArgumentFactory.Builder sargsFactory = SearchArgumentFactory.newBuilder();

    @Override
    public SearchArgument getSearchArgument() {
      return sargsFactory.build();
    }

  }

  static abstract class ValuePredicate extends BasePredicate {
    private static final long serialVersionUID = 1L;

    final Fields fields;
    final Comparator<Object> comparator;

    @SuppressWarnings("unchecked")
    ValuePredicate(Fields fields) {
      if (fields.size() != 1) {
        throw new IllegalArgumentException("Can only specify one field.");
      }
      this.fields = fields;
      comparator = fields.getComparators()[0];
    }

    @Override
    public void verify() {
    }

  }

  static abstract class SingleValuePredicate extends ValuePredicate {
    private static final long serialVersionUID = 1L;

    final Object value;

    SingleValuePredicate(Fields fields, Object value) {
      super(fields);
      this.value = value;
    }

    @Override
    public boolean apply(TupleEntry tupleEntry) {
      return applyValue(tupleEntry.getObject(fields));
    }

    abstract boolean applyValue(Object object);
  }

  interface CompositePredicate extends Predicate {
    void addPredicate(Predicate predicate);

    void end();
  }

  static abstract class ListCompositePredicate extends BasePredicate implements CompositePredicate {
    private static final long serialVersionUID = 1L;

    final List<Predicate> predicates = new ArrayList<>();

    @Override
    public void addPredicate(Predicate predicate) {
      predicates.add(predicate);
    }

    @Override
    public void verify() {
      if (predicates.size() < 2) {
        throw new IllegalStateException("Must contain at least two predicates");
      }
      for (Predicate predicate : predicates) {
        predicate.verify();
      }
    }

    @Override
    public void end() {
      sargsFactory.end();
    }

  }

  static abstract class SingletonCompositePredicate extends BasePredicate implements CompositePredicate {
    private static final long serialVersionUID = 1L;

    Predicate predicate;

    @Override
    public void addPredicate(Predicate predicate) {
      if (this.predicate != null) {
        throw new IllegalStateException("Predicate already set on not");
      }
      this.predicate = predicate;
    }

    @Override
    public void verify() {
      if (predicate == null) {
        throw new IllegalStateException("Must contain at least two predicates");
      }
      predicate.verify();
    }

    @Override
    public void end() {
      sargsFactory.end();
    }
  }

  static class BetweenPredicate extends ValuePredicate {
    private static final long serialVersionUID = 1L;
    private final Object lower;
    private final Object upper;

    BetweenPredicate(Fields fields, Object lower, Object upper) {
      super(fields);
      this.lower = lower;
      this.upper = upper;
      sargsFactory.between(fields, this.lower, this.upper);
    }

    @Override
    public boolean apply(TupleEntry tupleEntry) {
      Object value = tupleEntry.getObject(fields);
      // TODO Check this
      if (comparator.compare(value, lower) > 0) {
        return true;
      }
      // TODO Check this
      if (comparator.compare(value, upper) < 0) {
        return true;
      }
      return false;
    }
  }

  static class EqualsPredicate extends SingleValuePredicate {

    private static final long serialVersionUID = 1L;

    EqualsPredicate(Fields fields, Object value) {
      super(fields, value);
      sargsFactory.equals(fields, value);
    }

    @Override
    public boolean applyValue(Object value) {
      return this.value.equals(value);
    }

  }

  static class InPredicate extends ValuePredicate {

    private static final long serialVersionUID = 1L;

    final Object[] values;

    InPredicate(Fields fields, Object... values) {
      super(fields);
      this.values = values;
      sargsFactory.in(fields, values);
    }

    @Override
    public boolean apply(TupleEntry tupleEntry) {
      Object candidate = tupleEntry.getObject(fields);
      for (Object inValue : values) {
        if (candidate.equals(inValue)) {
          return true;
        }
      }
      return false;
    }

  }

  static class IsNullPredicate extends ValuePredicate {

    private static final long serialVersionUID = 1L;

    IsNullPredicate(Fields fields) {
      super(fields);
      sargsFactory.isNull(fields);
    }

    @Override
    public boolean apply(TupleEntry tupleEntry) {
      return tupleEntry.getObject(fields) == null;
    }

  }

  static class LessThanPredicate extends SingleValuePredicate {

    private static final long serialVersionUID = 1L;

    LessThanPredicate(Fields fields, Object value) {
      super(fields, value);
      sargsFactory.lessThan(fields, value);
    }

    @Override
    public boolean applyValue(Object value) {
      // TODO Check this
      return comparator.compare(value, this.value) < 0;
    }

  }

  static class LessThanEqualsPredicate extends SingleValuePredicate {

    private static final long serialVersionUID = 1L;

    LessThanEqualsPredicate(Fields fields, Object value) {
      super(fields, value);
      sargsFactory.lessThanEquals(fields, value);
    }

    @Override
    public boolean applyValue(Object value) {
      // TODO Check this
      return comparator.compare(value, this.value) <= 0;
    }

  }

  static class GreaterThanPredicate extends SingleValuePredicate {

    private static final long serialVersionUID = 1L;

    GreaterThanPredicate(Fields fields, Object value) {
      super(fields, value);
      sargsFactory.greaterThan(fields, value);
    }

    @Override
    public boolean applyValue(Object value) {
      // TODO Check this
      return comparator.compare(value, this.value) > 0;
    }

  }

  static class GreaterThanEqualsPredicate extends SingleValuePredicate {

    private static final long serialVersionUID = 1L;

    GreaterThanEqualsPredicate(Fields fields, Object value) {
      super(fields, value);
      sargsFactory.greaterThanEquals(fields, value);
    }

    @Override
    public boolean applyValue(Object value) {
      // TODO Check this
      return comparator.compare(value, this.value) >= 0;
    }

  }

  static class NullSafeEqualsPredicate extends SingleValuePredicate {

    private static final long serialVersionUID = 1L;

    NullSafeEqualsPredicate(Fields fields, Object value) {
      super(fields, value);
      sargsFactory.nullSafeEquals(fields, value);
    }

    @Override
    public boolean applyValue(Object value) {
      return Objects.equal(this.value, value);
    }

  }

  static class AndPredicate extends ListCompositePredicate {

    private static final long serialVersionUID = 1L;

    private final List<Predicate> predicates = new ArrayList<>();

    AndPredicate() {
      sargsFactory.startAnd();
    }

    @Override
    public boolean apply(TupleEntry tupleEntry) {
      for (Predicate predicate : predicates) {
        if (!predicate.apply(tupleEntry)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public void addPredicate(Predicate predicate) {
      predicates.add(predicate);
    }

  }

  static class OrPredicate extends ListCompositePredicate {

    private static final long serialVersionUID = 1L;

    OrPredicate() {
      sargsFactory.startOr();
    }

    @Override
    public boolean apply(TupleEntry tupleEntry) {
      for (Predicate predicate : predicates) {
        if (predicate.apply(tupleEntry)) {
          return true;
        }
      }
      return false;
    }

  }

  static class NotPredicate extends SingletonCompositePredicate {

    private static final long serialVersionUID = 1L;

    NotPredicate() {
      sargsFactory.startNot();
    }

    @Override
    public boolean apply(TupleEntry tupleEntry) {
      return !predicate.apply(tupleEntry);
    }

  }

  static class IdentityPredicate extends SingletonCompositePredicate {

    private static final long serialVersionUID = 1L;

    IdentityPredicate() {
    }

    @Override
    public boolean apply(TupleEntry tupleEntry) {
      return predicate.apply(tupleEntry);
    }

    @Override
    public void end() {
    }

  }

}
