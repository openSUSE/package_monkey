# Composition Files and Tools

Once you have a working classification, the last step for assembling a product is the actual
composition. This step generally does not operate on individual RPMs any longer, but lets you
build the product by including or excluding logical building blocks (aka epics), and their
dependencies.

## The ``compose.yaml`` File

When running `monkey compose`, it will load the `compose.yaml` file corresponding to the default
release of the codebase. For SLFO:Main, right now the target release is `sle16.1` (this is defined
in `slfo.yaml`), so the composer will load `sle16.1/compose.yaml`.

If you look at the file, you will see a `products` section that contains several subsections, called
`defaults`, `sles_16.1`, `sleha_16.1` etc. The `defaults` section does what the name indicates, it
provides default settings for all products defined by this file; the other sections describe the
individual products.

### The `defaults` Section

Let's take a look at the `defaults` section.

```
products:
    defaults:
        architectures:
         - x86_64
         - aarch64
         - s390x
         - ppc64le

        classes:
         default:       include support=l3
         libraries:     include support=l3
         runtime:       include support=l3
         x86_64_v3:     include support=l3
         user:          include support=l3
         important:     include support=l3
         api:           include support=l2
         x86_64_v3_api: include support=l2
         apidoc:        include support=l2
         doc:           include support=l2
         man:           include support=l2
         i18n:          include support=l2
         32bit:         exclude
         32bit_api:     exclude
         noship:        exclude
         test:          exclude
         unresolved:    exclude

        options:
         # things marked by option=obs exist exclusively for building
         obs:           exclude

        layers:
         LayerCore:
           epics:
             Unresolved: exclude
```

The first setting is the list of architectures we build our products for by default (of course, a product
like SLES for SAP would override this, as it is not being shipped for a subset of architectures only).

The second attribute is `classes`, which tells the composer which rpm classes can be included, and which ones
definitely shouldn't be included. This is the first block of *policy statements* in the yaml file. Policies
primarily state what gets included in the product, and what doesn't; plus it may specify modifiers that
influence *how* something is included.

As we're not supporting 32bit systems any longer, we exclude `32bit` and `32bit_api`.  Likewise for
`test`, which comprises packages that are used exclusively for internal testing purposes.  We also
exclude RPMs that have unresolved dependencies (no surprise here).

The list also mentions `noship`, which is worth explaining briefly. Currently, the `noship` and `private`
rpm annotations are handled using special RPM classes. However, this is an implementation detail which
may change in the future, so you should not rely on this fact.

`important` is instead used to mark some packages as important. If for some reason important packages
are excluded for whatever reason (such as a new unclassified dependency), the compose call would fail,
making it obvious that something is wrong.  
Can be useful for pattern or release packages, where we should really ensure they get in.

In addition to the verdict (`include` vs `exclude`), you can see, in addition, the support level per
rpm class. In this example, we provide full Level 3 support for classes that relate to running an
application, while development packages and documentation are Level 2 supported only.

Next, the `options` attribute specifies how certain options are used. In this example, the `defaults`
section only mentions the `obs` build option. This option is used to tag rpms that exist only because
they are used for building other packages; so by definition, we should exclude it from shipping.

Last but not least, the `layers` attribute specifies layers (and labels nested within them, like epics
and extras), and whether they shall be included or excluded. Again, these are *policy statements*, and
we'll take a deeper look at their syntax in a few moments.

### Products and their attributes

The `defaults` section is followed by one or more product definitions. This definition mainly
consists of a few attributes that describe key product properties, followed by `options`
and `layers` sections just like in `defaults`. Here's what the start of SLES 16.1 looks like:

```
sles_16.1:
    name:           SLES16.1
    obs_composekey: sles
    release_epic:   SLESProduct
    contracts:
     - general
     - lts
    
    options:
     SLES:          include
     opengl:        include
     ...

    layers:
     LayerCore:     include
     ...
```

The value of `obs_composekey` is used when generating `default.productcompose`.
The `name` attribute is, obviously, the product name, which is used in several output
files.

The `contracts` list is used in generating lifecycle data; and we will cover it in
that context.

The `release_epic` is the name of an epic that contains packages like `SLES-release`.
These epics receive special treatment due to the way we generate extensions and
derived products.

### Extensions and Derived Products

An extension is a product that is installed on top of a base product, such as SLE HA,
which looks like this in `compose.yaml`:

```
sleha_16.1:
    name:           SLE HA 16.1
    obs_composekey: sles_ha
    release_epic:   HAExtension
    extend:         sles_16.1
    contracts:
     - general
     - lts
```

The keyword `extend` signals that it is an extension of the base product `sles_16.1`.

The composer handles extensions by combining the policy statements from the base product
with the policy statements of the extension, and performing a full composition. Finally,
it uses the set of rpms that result from this composition, and subtracts the set of rpms
that make up the base product.

A product like SLES for SAP is not an extension, but basically copies everything from
a base product (SLES in this case) and adds things on top. Its definition looks like this:

```
sles_sap_16.1:
    name:           SLE for SAP 16.1
    obs_composekey: sles_sap
    release_epic:   SAPProduct
    copy:           sles_16.1
    contracts:
     - general
     - lts

    architectures:
     - ppc64le
     - x86_64
```

The keyword `copy` tells the composer that this product is deried from its base product.

Again, the composer handles derived products by combining the policy statements from the
base product with the policy statements of the extension, and performing a full composition.
The set of rpms coming out of this composition is what makes up the derived product.

In this context, it becomes clear why we handle `*-release` packages via a special mechanism
outside of the regular policy statements; because a derived product like SLES for SAP should
include *only* its own release package(s), but not those of the base product it is
derived from.

Note how SLES for SAP overrides the `architectures` specified in the `defaults` section;
we provide this product on these architectures only.

### Policy Statements

In essence, a policy statement says something about labels being included or excluded. In terms of
YAML syntax, policy statements are given as a mapping, i.e. key value pairs, where the key is the
name of the label, and the value describes the policy.

In its simplest form, the policy is a string (`exclude` or `include`), so it looks something like this:

```
foo_label: include
bar_label: exclude
```

Depending on the context, the file parser expects different types of labels; so within the `options`
section, you can only have build options; within the `layers` section you can have only layer labels; etc.

The simple example above looks as if the verdict on a label is somewhat binary, being either `included`
or `excluded`. However, there is actually a third state, which is the default state, referred to
`asneeded`. If we leave a label without a decision, it stays in this default state, allowing packages
from this label to be pulled in via dependencies (i.e. as needed).

#### Complex Policy Statements

Now, it would not be very flexible if all we could do is control the inclusion or exclusion of layers,
or epics for that matter. So in addition to the simple form above, the policy part can be more complex,
being a YAML mapping itself.

To begin with, the following two examples do exactly the same thing:

```
 foo_label:             include
 foo_label:
   self:                include
```

The second is just a different notation.

Here's a more complex example, telling the composer how to handle the layer `LayerSecurity`:

```
 LayerSecurity:
   self:                include
   epics:
     Heimdal:           exclude
     SmartcardCoolkey:  exclude
```

The `self` attribute specifies the verdict for the layer itself. This setting is applied to
all epics within this layer, unless specifically overridden. In this example, we have marked
the epics that control the Heimdal kerberos implementation and the Coolkey smart card code
(which is really just too old).

#### Architecture Constraints

A policy statement can provide constraints on the architecture of packages included. For example, there
are quite a few cases where we build packages for more architectures than is really warranted. In
other cases, we may be building packages for architectures on which we're not yet ready to support
the code.

```
 LayerUpdateStack:
   self:                include
   epics:
     UserspaceLivePatching:
       architectures:
        - x86_64
        - ppc64le
```

In this example, we constrain the epic for user space live patching to AMD/Intel and IBM POWER,
because we do not plan to support the stack on other architectures yet.

#### Applying Policy to Extras

Lastly, here are two (equivalent) examples that show how to handle extras:

```
 LayerSecurity:
   self:                include
   epics:
     PGP:
        self:           include
	extras:
	  tpm:          include

 LayerSecurity:
   self:                include
   epics:
     PGP:
	extra.tpm:      include
```

Both include PGP along with `PGP+tpm`, which contains some package(s) allowing gnupg to use the
TPM chip for some crypto operations. `extra.tpm` is just shorthand for the `extras` section shown
in the first example. Also, the second example omits the `self` statement within `PGP` because it
already defaults to `include` thanks to the setting at the layer level.

#### Closure Rules

Closure Rules are a special topic which we will explain in more detail. For now, suffice it to
say that you can also specify closure rules within policy statements like this:


```
 LayerWorkstationServices:
   self:                include
   epics:
     PDF:
       closure:         noapi
```

### How Policy is applied

In order to understand how the policy statements impact the product composition in detail, we
need to take a look at the underlying algorithm.

1. Policy statements are converted to a tree in memory, with a so-called default policy at the
   top, containing layer policies, containing epic policies, which in turn contains build options
   and extras. The default policy holds the architecture and class settings from the `defaults`
   section of the `compose.yaml` file.
2. Settings are percolated down the tree, unless overridden by values given in the yaml file.
   - In particular, if an epic is enabled, so are any build options it defines.
     The only exception are extras; these are not enabled automatically.
   - Should there be any epics that do not belong to a layer, they will be warned about,
     and the default policy will be applied.
3. In the first pass, we loop over all build options that have been marked as `excluded`. We
   exclude the epics that they are part of. When excluding an epic, this means we exclude
   any rpms that belong to it, as well as any rpms that depend on them.
   If there are conflicting settings (e.g. an epic is marked as `include` by policy statement,
   but a build option defined by it is marked as `exclude`), this is an error and the tool
   will raise an exception.
4. In the next pass, we look at epics that have been marked as depending on specific options
   (via `required_options`). If an epic depends on a disabled option, the epic is disabled.
   If an enabled epic depends on an option for which the user has not set a policy, the
   option is enabled, too.
5. We loop over all epics and check their extras to see whether they have been enabled or
   disabled explicitly. If an epic is excluded but one of its extras is included, this is
   an error that will result in an exception.
6. We loop over labels excluded by one of the passes above (epics, options, extras), and
   mark the rpms that belong to them as excluded.
7. We loop over epics marked as included, and include all member rpms suggested by the
   closure rules attached to the epic. As we include an rpm, we include any rpm they depend
   on.
8. We loop over epics for which there is no decision (i.e. they're still in state `asneeded`)
   and check if any of their rpms have been included. If so, we consult the corresponding
   closure rules to tell us what to include along with the rpm.

#### Closure Rules

Closure rules specify what exactly happens with the rpms within an epic when it gets included in the
composition, or parts of it. There is more than one set of closure rules, as we may want to treat
different parts of our product differently.

A closure rule consists of two parts, a *set of requested classes*, and a map of *complementing
classes*.

The first set applies when an epic is included. This is simply a list of rpm classes that
shall be included. The default closure rules just include all allowed classes (ie excluding
things that we already excluded in the `defaults` section, like e.g. `32bit` class packages).
However, there is also a rule set called `noapi`, which omits any development packages (such
packages could still be pulled in via dependencies, but we would not include them by default).
There is even a closure rule set specifically for multimedia related epics, which is even
more restrictive and essentially focuses on the shared libraries.

The second part, the map of complementing classes, comes into effect when dealing with epics
that are neither included nor excluded. We may still end up included packages from these
(via rpm dependencies), in which case we want to apply some standard rules such as "if we
ship a library, and there is an x86_64_v3 optimized counterpart, include it as well"; or
"if we ship an application, ship the manpages that go with it". These kinds of rules are
implemented by a relation on the rpm classes. This is, admittedly, a bit coarse, but works
well enough so far.

The closure rules are also defined in `compose.yaml`, in the `closure_rules` section.

### RPM Overrides

Sometimes, making the classifier and composer do exactly what you want can be tedious
and complicated, while you may be in a hurry to make a single change stick with hours
to go before the RC1 deadline. This is why RPM overrides exist. This section can be used
within any product section, and specifies individual RPMs to be included or excluded.

```
override_rpms:
 include:
  - libpmem-devel: [x86_64, ppc64le]
  - libpmempool-devel: [x86_64, ppc64le]
  - libpmem2-devel: [x86_64]
  - python313-process-tests

 exclude:
  - python313-pyproject-api
  - pcre-doc
```

RPMs listed in the `include` section will be added to the composition, or will have
their list of architectures adjusted. RPMs listed in the `exclude` section will be
removed from the composition.

Note that there is no wildcarding in these override lists; so do not rely too much on
them. They become messy very quickly.
